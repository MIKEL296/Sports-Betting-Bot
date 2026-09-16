import os
import time
import math
import json
import logging
import aiohttp
import asyncio
import aiosqlite
from typing import List, Dict, Any, Optional, Tuple
from datetime import datetime, timezone, timedelta
from dotenv import load_dotenv

from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.request import HTTPXRequest
from telegram.ext import (
    ApplicationBuilder,
    CommandHandler,
    CallbackQueryHandler,
    ContextTypes
)

# -------------------------------------------------------------------
# Configuration Setup
# -------------------------------------------------------------------
load_dotenv()

TELEGRAM_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
ODDS_API_KEY = os.getenv("ODDS_API_KEY")
BASE_URL = "https://api.the-odds-api.com/v4/sports"
DB_NAME = "todays_predictions.db"

logging.basicConfig(format="%(asctime)s - %(name)s - %(levelname)s - %(message)s", level=logging.INFO)

# High-liquidity worldwide leagues to guarantee 40-80+ fixtures every single day
ALL_ACTIVE_SOCCER_LEAGUES = {
    # European Cups & Top Leagues
    "soccer_uefa_champs_league": "UEFA Champions League",
    "soccer_uefa_europa_league": "UEFA Europa League",
    "soccer_uefa_conference_league": "UEFA Conference League",
    "soccer_epl": "Premier League (ENG)",
    "soccer_efl_champ": "Championship (ENG)",
    "soccer_england_efl_cup": "EFL Cup (ENG)",
    "soccer_spain_la_liga": "La Liga (ESP)",
    "soccer_spain_segunda_division": "La Liga 2 (ESP)",
    "soccer_germany_bundesliga": "Bundesliga (GER)",
    "soccer_italy_serie_a": "Serie A (ITA)",
    "soccer_france_ligue_one": "Ligue 1 (FRA)",
    "soccer_netherlands_eredivisie": "Eredivisie (NED)",
    "soccer_portugal_primeira_liga": "Primeira Liga (POR)",
    "soccer_turkey_super_league": "Super Lig (TUR)",
    "soccer_belgium_first_div": "Pro League (BEL)",
    "soccer_scotland_premiership": "Premiership (SCO)",
    # Active Americas & Asian Leagues (Ensures daily matches year-round)
    "soccer_brazil_campeonato": "Série A (BRA)",
    "soccer_argentina_primera_division": "Primera (ARG)",
    "soccer_mexico_ligamx": "Liga MX (MEX)",
    "soccer_usa_mls": "MLS (USA)",
    "soccer_japan_j_league": "J-League (JPN)",
    "soccer_sweden_allsvenskan": "Allsvenskan (SWE)",
    "soccer_norway_eliteserien": "Eliteserien (NOR)"
}

def clean_md(text: str) -> str:
    if not text:
        return ""
    for char in ["_", "*", "`", "[", "]", "(", ")"]:
        text = text.replace(char, " ")
    return " ".join(text.split())

# -------------------------------------------------------------------
# Probability Devigging
# -------------------------------------------------------------------
def devig_power_method(odds_list: List[float]) -> List[float]:
    if not odds_list or any(o <= 1.0 for o in odds_list):
        return []
    raw_probs = [1.0 / o for o in odds_list]
    overround = sum(raw_probs)
    if abs(overround - 1.0) < 0.001:
        return raw_probs

    low, high = 1.0, 3.0
    k = 1.0
    for _ in range(25):
        mid = (low + high) / 2.0
        val = sum(math.pow(p, mid) for p in raw_probs)
        if val > 1.0:
            low = mid
        else:
            high = mid
        k = mid

    fair_probs = [math.pow(p, k) for p in raw_probs]
    total_fair = sum(fair_probs)
    return [p / total_fair for p in fair_probs]

# -------------------------------------------------------------------
# Database Architecture
# -------------------------------------------------------------------
async def init_db():
    async with aiosqlite.connect(DB_NAME) as db:
        await db.execute("""
            CREATE TABLE IF NOT EXISTS daily_fixtures (
                fixture_id TEXT PRIMARY KEY,
                league_key TEXT,
                league_name TEXT,
                home_team TEXT,
                away_team TEXT,
                home_odds REAL,
                draw_odds REAL,
                away_odds REAL,
                commence_time TEXT,
                fetch_date TEXT
            )
        """)
        await db.commit()
    logging.info("Database initialized.")

async def store_fixtures_to_db(fixtures_data: List[Dict[str, Any]]):
    today_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    async with aiosqlite.connect(DB_NAME) as db:
        for f in fixtures_data:
            await db.execute("""
                INSERT INTO daily_fixtures (
                    fixture_id, league_key, league_name, home_team, away_team, 
                    home_odds, draw_odds, away_odds, commence_time, fetch_date
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(fixture_id) DO UPDATE SET
                    home_odds=excluded.home_odds,
                    draw_odds=excluded.draw_odds,
                    away_odds=excluded.away_odds,
                    commence_time=excluded.commence_time,
                    fetch_date=excluded.fetch_date
            """, (
                f["id"], f["league_key"], f["league_name"], f["home_team"], f["away_team"],
                f["home_odds"], f["draw_odds"], f["away_odds"], f["commence_time"], today_str
            ))
        await db.commit()

async def get_cached_fixtures_count() -> int:
    today_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    async with aiosqlite.connect(DB_NAME) as db:
        async with db.execute("SELECT COUNT(*) FROM daily_fixtures WHERE fetch_date = ?", (today_str,)) as cursor:
            row = await cursor.fetchone()
            return row[0] if row else 0

async def load_cached_fixtures() -> List[Dict[str, Any]]:
    today_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    async with aiosqlite.connect(DB_NAME) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute("SELECT * FROM daily_fixtures WHERE fetch_date = ?", (today_str,)) as cursor:
            rows = await cursor.fetchall()
            return [dict(r) for r in rows]

# -------------------------------------------------------------------
# Module 1: READ (Ingest Same-Day Matches in 24-Hour Horizon)
# -------------------------------------------------------------------
async def run_read_and_store_pipeline() -> Dict[str, Any]:
    if not ODDS_API_KEY:
        return {"success": False, "message": "ODDS_API_KEY is missing in your .env file."}

    now_utc = datetime.now(timezone.utc)
    window_start = now_utc - timedelta(hours=1)
    window_end = now_utc + timedelta(hours=28)  # Full 24-28 hour kickoff range

    normalized_fixtures = []

    async with aiohttp.ClientSession() as session:
        sem = asyncio.Semaphore(5)

        async def fetch_league(sport_key: str, label: str):
            async with sem:
                url = f"{BASE_URL}/{sport_key}/odds/"
                params = {
                    "apiKey": ODDS_API_KEY,
                    "regions": "eu,uk",
                    "markets": "h2h",
                    "oddsFormat": "decimal"
                }
                try:
                    async with session.get(url, params=params, timeout=12) as resp:
                        if resp.status == 200:
                            data = await resp.json()
                            results = []
                            for fixture in data:
                                commence_raw = fixture.get("commence_time", "")
                                if not commence_raw:
                                    continue

                                commence_dt = datetime.fromisoformat(commence_raw.replace('Z', '+00:00'))
                                if not (window_start <= commence_dt <= window_end):
                                    continue

                                bookies = fixture.get("bookmakers", [])
                                if not bookies:
                                    continue

                                h2h = next((m for b in bookies for m in b.get("markets", []) if m.get("key") == "h2h"), None)
                                if not h2h:
                                    continue

                                outcomes = h2h.get("outcomes", [])
                                home_name = fixture.get("home_team")
                                away_name = fixture.get("away_team")

                                home_o = next((o["price"] for o in outcomes if o["name"] == home_name), None)
                                draw_o = next((o["price"] for o in outcomes if o["name"] == "Draw"), None)
                                away_o = next((o["price"] for o in outcomes if o["name"] == away_name), None)

                                if home_o and away_o:
                                    results.append({
                                        "id": fixture["id"],
                                        "league_key": sport_key,
                                        "league_name": label,
                                        "home_team": home_name,
                                        "away_team": away_name,
                                        "home_odds": float(home_o),
                                        "draw_odds": float(draw_o) if draw_o else 3.25,
                                        "away_odds": float(away_o),
                                        "commence_time": commence_raw
                                    })
                            return results
                except Exception as e:
                    logging.warning(f"Error fetching {label}: {e}")
                return []

        tasks = [fetch_league(k, v) for k, v in ALL_ACTIVE_SOCCER_LEAGUES.items()]
        batch_results = await asyncio.gather(*tasks)
        for res in batch_results:
            normalized_fixtures.extend(res)

    if normalized_fixtures:
        await store_fixtures_to_db(normalized_fixtures)
        return {"success": True, "count": len(normalized_fixtures), "leagues": len(ALL_ACTIVE_SOCCER_LEAGUES)}

    return {"success": False, "message": "Zero matches returned. Verify lines are active."}

# -------------------------------------------------------------------
# Module 2: PREDICT (2 Distinct ~5.0 Odds Same-Day Tickets)
# -------------------------------------------------------------------
def evaluate_candidate(f: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    prices = [f["home_odds"], f["draw_odds"], f["away_odds"]]
    probs = devig_power_method(prices)
    if len(probs) < 3:
        return None

    home_p, draw_p, away_p = probs[0], probs[1], probs[2]
    candidates = []

    # 1. Straight Win Favorites (Odds: 1.25 to 1.68, Min Prob: 55%)
    if home_p >= 0.55 and 1.25 <= f["home_odds"] <= 1.68:
        candidates.append({
            "pick": f"{clean_md(f['home_team'])} (Home Win)",
            "odds": f["home_odds"],
            "prob": home_p
        })
    elif away_p >= 0.55 and 1.25 <= f["away_odds"] <= 1.68:
        candidates.append({
            "pick": f"{clean_md(f['away_team'])} (Away Win)",
            "odds": f["away_odds"],
            "prob": away_p
        })

    # 2. Resilient Double Chance (Odds: 1.16 to 1.42, Min Prob: 66%)
    p_1x = home_p + draw_p
    if p_1x >= 0.66:
        dc_odds = round(1.0 / (p_1x * 1.05), 2)
        if 1.16 <= dc_odds <= 1.42:
            candidates.append({
                "pick": f"{clean_md(f['home_team'])} or Draw (1X)",
                "odds": dc_odds,
                "prob": p_1x
            })

    p_x2 = away_p + draw_p
    if p_x2 >= 0.66:
        dc_odds = round(1.0 / (p_x2 * 1.05), 2)
        if 1.16 <= dc_odds <= 1.42:
            candidates.append({
                "pick": f"Draw or {clean_md(f['away_team'])} (X2)",
                "odds": dc_odds,
                "prob": p_x2
            })

    if not candidates:
        return None

    candidates.sort(key=lambda x: x["prob"], reverse=True)
    best = candidates[0]
    best["fixture_id"] = f["fixture_id"]
    best["fixture"] = f"{f['home_team']} vs {f['away_team']}"
    best["league_name"] = f["league_name"]
    best["raw_market"] = f"H: {f['home_odds']} | D: {f['draw_odds']} | A: {f['away_odds']}"

    try:
        dt = datetime.fromisoformat(f["commence_time"].replace('Z', '+00:00'))
        best["kickoff"] = dt.strftime("%H:%M UTC")
    except Exception:
        best["kickoff"] = "Today"

    return best

async def build_dual_5_odds_slips() -> List[str]:
    cached_matches = await load_cached_fixtures()
    if not cached_matches:
        return ["⚠️ *Database empty for today.* Tap **📖 Read Today's Matches** first."]

    evaluated = []
    for f in cached_matches:
        cand = evaluate_candidate(f)
        if cand:
            evaluated.append(cand)

    # Rank candidate picks by confidence
    evaluated.sort(key=lambda x: x["prob"], reverse=True)

    if len(evaluated) < 6:
        return [
            f"⚠️ *Not enough qualifying matches today* ({len(evaluated)} found).\n\n"
            "Tap **📖 Read Today's Matches** later when bookmakers post more lines."
        ]

    # Partition candidates across Slip 1 and Slip 2
    # Allow up to 2 matches per league across the whole day so big tournament nights aren't discarded
    slip_1_legs = []
    slip_2_legs = []
    league_counts_1 = {}
    league_counts_2 = {}
    odds_1 = 1.0
    odds_2 = 1.0
    used_fixtures = set()

    # Pass 1: Build Slip 1 to ~5.0 odds
    for leg in evaluated:
        if leg["fixture_id"] in used_fixtures:
            continue
        l_name = leg["league_name"]
        if league_counts_1.get(l_name, 0) >= 2:
            continue
        if (odds_1 * leg["odds"]) > 6.8:
            continue

        slip_1_legs.append(leg)
        league_counts_1[l_name] = league_counts_1.get(l_name, 0) + 1
        used_fixtures.add(leg["fixture_id"])
        odds_1 *= leg["odds"]

        if 4.8 <= odds_1 <= 6.8:
            break

    # Pass 2: Build Slip 2 to ~5.0 odds with remaining fixtures
    for leg in evaluated:
        if leg["fixture_id"] in used_fixtures:
            continue
        l_name = leg["league_name"]
        if league_counts_2.get(l_name, 0) >= 2:
            continue
        if (odds_2 * leg["odds"]) > 6.8:
            continue

        slip_2_legs.append(leg)
        league_counts_2[l_name] = league_counts_2.get(l_name, 0) + 1
        used_fixtures.add(leg["fixture_id"])
        odds_2 *= leg["odds"]

        if 4.8 <= odds_2 <= 6.8:
            break

    messages = []
    today_str = datetime.now(timezone.utc).strftime("%a, %d %b %Y")

    # Format Slip 1
    if len(slip_1_legs) >= 3 and odds_1 >= 3.8:
        card_1 = [
            f"🎯 *SAME-DAY SLIP 1 — (~5.0 ODDS)*",
            f"📅 Date: `{today_str}`",
            f"📈 Total Odds: `{odds_1:.2f}`",
            f"⚡ Cashout: `All matches finish today`",
            "───────────────────────────\n"
        ]
        for idx, leg in enumerate(slip_1_legs, 1):
            card_1.append(
                f"*{idx}. {clean_md(leg['fixture'])}* (`{leg['kickoff']}`)\n"
                f"🏆 _{clean_md(leg['league_name'])}_\n"
                f"🎲 1X2: `{leg['raw_market']}`\n"
                f"🔒 Pick: *{leg['pick']}* @ `{leg['odds']:.2f}`\n"
                f"🛡️ Safety: `{(leg['prob']*100):.1f}% Confidence`\n"
            )
        card_1.append("───────────────────────────")
        messages.append("\n".join(card_1))
    else:
        messages.append("⚠️ *Slip 1:* Unable to hit ~5.0 odds with current safe picks.")

    # Format Slip 2
    if len(slip_2_legs) >= 3 and odds_2 >= 3.8:
        card_2 = [
            f"🎯 *SAME-DAY SLIP 2 — (~5.0 ODDS)*",
            f"📅 Date: `{today_str}`",
            f"📈 Total Odds: `{odds_2:.2f}`",
            f"⚡ Cashout: `Zero overlap with Slip 1`",
            "───────────────────────────\n"
        ]
        for idx, leg in enumerate(slip_2_legs, 1):
            card_2.append(
                f"*{idx}. {clean_md(leg['fixture'])}* (`{leg['kickoff']}`)\n"
                f"🏆 _{clean_md(leg['league_name'])}_\n"
                f"🎲 1X2: `{leg['raw_market']}`\n"
                f"🔒 Pick: *{leg['pick']}* @ `{leg['odds']:.2f}`\n"
                f"🛡️ Safety: `{(leg['prob']*100):.1f}% Confidence`\n"
            )
        card_2.append("───────────────────────────")
        messages.append("\n".join(card_2))
    else:
        messages.append("ℹ️ *Slip 2:* Need more distinct match fixtures today to generate the second non-overlapping ticket.")

    return messages

# -------------------------------------------------------------------
# Telegram Handlers
# -------------------------------------------------------------------
def build_main_keyboard(cached_count: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton("📖 Read Today's Matches", callback_data="btn_read"),
            InlineKeyboardButton(f"🔮 Predict 2x 5-Odds ({cached_count})", callback_data="btn_predict")
        ]
    ])

async def start_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await init_db()
    count = await get_cached_fixtures_count()
    await update.message.reply_text(
        "⚽ *Daily 2x 5-Odds Accumulator Hub*\n\n"
        "• **📖 Read Today's Matches**: Pulls live games across European and global competitions kicking off today into SQLite[span_3](start_span)[span_3](end_span).\n"
        "• **🔮 Predict**: Builds **2 separate, non-overlapping ~5.0 odds tickets** that conclude today for same-day cashout.",
        parse_mode="Markdown",
        reply_markup=build_main_keyboard(count)
    )

async def button_router(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    data = query.data
    chat_id = query.message.chat_id

    if data == "btn_read":
        status = await context.bot.send_message(
            chat_id=chat_id,
            text="⏳ *Reading all matches kicking off in the next 24 hours into SQLite...*",
            parse_mode="Markdown"
        )
        res = await run_read_and_store_pipeline()
        await status.delete()

        count = await get_cached_fixtures_count()
        if res.get("success"):
            text = (
                f"✅ *Same-Day Matches Synchronized!*\n\n"
                f"Stored `{res['count']}` matches across `{res['leagues']}` competitions into SQLite.\n\n"
                f"Tap **🔮 Predict 2x 5-Odds** to generate your tickets."
            )
        else:
            text = f"⚠️ *Notice:* {res.get('message')}"

        await context.bot.send_message(chat_id=chat_id, text=text, parse_mode="Markdown", reply_markup=build_main_keyboard(count))

    elif data == "btn_predict":
        count = await get_cached_fixtures_count()
        if count == 0:
            await context.bot.send_message(
                chat_id=chat_id,
                text="⚠️ No fixtures cached. Tap **📖 Read Today's Matches** first.",
                parse_mode="Markdown",
                reply_markup=build_main_keyboard(0)
            )
            return

        status = await context.bot.send_message(
            chat_id=chat_id,
            text="⚙️ *Constructing 2 separate same-day ~5.0 odds tickets...*",
            parse_mode="Markdown"
        )
        reports = await build_dual_5_odds_slips()
        await status.delete()

        for report_text in reports:
            await context.bot.send_message(chat_id=chat_id, text=report_text, parse_mode="Markdown")

# -------------------------------------------------------------------
# Entry Point
# -------------------------------------------------------------------
def main():
    if not TELEGRAM_TOKEN:
        raise ValueError("TELEGRAM_BOT_TOKEN is missing!")

    asyncio.run(init_db())

    request_config = HTTPXRequest(
        connection_pool_size=10,
        connect_timeout=35.0,
        read_timeout=35.0,
        write_timeout=35.0,
        pool_timeout=35.0
    )

    app = (
        ApplicationBuilder()
        .token(TELEGRAM_TOKEN)
        .request(request_config)
        .get_updates_request(request_config)
        .build()
    )

    app.add_handler(CommandHandler("start", start_command))
    app.add_handler(CallbackQueryHandler(button_router))

    print("🚀 Bot running with high-pool daily accumulator engine...")
    app.run_polling(drop_pending_updates=True)

if __name__ == "__main__":
    main()
