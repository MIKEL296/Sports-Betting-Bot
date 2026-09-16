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
# Environment & Configuration Setup
# -------------------------------------------------------------------
load_dotenv()

TELEGRAM_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
ODDS_API_KEY = os.getenv("ODDS_API_KEY")
BASE_URL = "https://api.the-odds-api.com/v4/sports"
DB_NAME = "todays_predictions.db"

logging.basicConfig(format="%(asctime)s - %(name)s - %(levelname)s - %(message)s", level=logging.INFO)

# Prioritized European Tier-1/Tier-2 & UEFA Competitions
EURO_SOCCER_LEAGUES = {
    # UEFA Midweek Cups (Champions League, Europa, Conference)
    "soccer_uefa_champs_league": "UEFA Champions League",
    "soccer_uefa_europa_league": "UEFA Europa League",
    "soccer_uefa_conference_league": "UEFA Europa Conference League",

    # England
    "soccer_epl": "Premier League (ENG)",
    "soccer_efl_champ": "Championship (ENG)",
    "soccer_england_efl_cup": "EFL Cup (ENG)",
    "soccer_fa_cup": "FA Cup (ENG)",

    # Spain
    "soccer_spain_la_liga": "La Liga (ESP)",
    "soccer_spain_segunda_division": "Segunda Division (ESP)",

    # Germany
    "soccer_germany_bundesliga": "Bundesliga (GER)",
    "soccer_germany_bundesliga2": "Bundesliga 2 (GER)",

    # Italy
    "soccer_italy_serie_a": "Serie A (ITA)",
    "soccer_italy_serie_b": "Serie B (ITA)",

    # France
    "soccer_france_ligue_one": "Ligue 1 (FRA)",
    "soccer_france_ligue_two": "Ligue 2 (FRA)",

    # Top European Competitions
    "soccer_netherlands_eredivisie": "Eredivisie (NED)",
    "soccer_portugal_primeira_liga": "Primeira Liga (POR)",
    "soccer_belgium_first_div": "Pro League (BEL)",
    "soccer_turkey_super_league": "Super Lig (TUR)",
    "soccer_scotland_premiership": "Premiership (SCO)"
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
# Database Architecture (Local Match & Slip Cache)
# -------------------------------------------------------------------
async def init_db():
    async with aiosqlite.connect(DB_NAME) as db:
        await db.execute("""
            CREATE TABLE IF NOT EXISTS euro_fixtures (
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
        await db.execute("""
            CREATE TABLE IF NOT EXISTS accumulator_slips (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                created_date TEXT,
                slip_label TEXT,
                total_odds REAL,
                legs_count INTEGER,
                legs_summary TEXT,
                timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
            )
        """)
        await db.commit()
    logging.info("SQLite database synchronized.")

async def store_fixtures_to_db(fixtures_data: List[Dict[str, Any]]):
    today_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    async with aiosqlite.connect(DB_NAME) as db:
        for f in fixtures_data:
            await db.execute("""
                INSERT INTO euro_fixtures (
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
        async with db.execute("SELECT COUNT(*) FROM euro_fixtures WHERE fetch_date = ?", (today_str,)) as cursor:
            row = await cursor.fetchone()
            return row[0] if row else 0

async def load_cached_fixtures() -> List[Dict[str, Any]]:
    today_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    async with aiosqlite.connect(DB_NAME) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute("SELECT * FROM euro_fixtures WHERE fetch_date = ?", (today_str,)) as cursor:
            rows = await cursor.fetchall()
            return [dict(r) for r in rows]

# -------------------------------------------------------------------
# Module 1: READ (Ingest European Fixtures across 7 Days)
# -------------------------------------------------------------------
async def run_read_and_store_pipeline() -> Dict[str, Any]:
    if not ODDS_API_KEY:
        return {"success": False, "message": "ODDS_API_KEY is missing in your .env file."}

    now_utc = datetime.now(timezone.utc)
    window_start = now_utc - timedelta(hours=2)
    window_end = now_utc + timedelta(days=7)  # Captures midweek UEFA & upcoming weekend matches

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
                                        "draw_odds": float(draw_o) if draw_o else 3.30,
                                        "away_odds": float(away_o),
                                        "commence_time": commence_raw
                                    })
                            return results
                except Exception as e:
                    logging.warning(f"Error fetching {label}: {e}")
                return []

        tasks = [fetch_league(k, v) for k, v in EURO_SOCCER_LEAGUES.items()]
        batch_results = await asyncio.gather(*tasks)
        for res in batch_results:
            normalized_fixtures.extend(res)

    if normalized_fixtures:
        await store_fixtures_to_db(normalized_fixtures)
        return {"success": True, "count": len(normalized_fixtures), "leagues": len(EURO_SOCCER_LEAGUES)}

    return {"success": False, "message": "Zero European matches found. Ensure matches have posted lines."}

# -------------------------------------------------------------------
# Module 2: PREDICT (2 Distinct 10.0+ Odds European Accumulators)
# -------------------------------------------------------------------
def evaluate_candidate_options(f: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    prices = [f["home_odds"], f["draw_odds"], f["away_odds"]]
    probs = devig_power_method(prices)
    if len(probs) < 3:
        return None

    home_p, draw_p, away_p = probs[0], probs[1], probs[2]
    candidates = []

    # 1. Straight Favorites (Odds: 1.25 - 1.65)
    if home_p >= 0.58 and 1.25 <= f["home_odds"] <= 1.65:
        candidates.append({
            "pick": f"{clean_md(f['home_team'])} to Win",
            "type": "1X2",
            "odds": f["home_odds"],
            "prob": home_p
        })
    elif away_p >= 0.58 and 1.25 <= f["away_odds"] <= 1.65:
        candidates.append({
            "pick": f"{clean_md(f['away_team'])} to Win",
            "type": "1X2",
            "odds": f["away_odds"],
            "prob": away_p
        })

    # 2. High Stability Double Chance (Odds: 1.18 - 1.40)
    p_1x = home_p + draw_p
    if p_1x >= 0.67:
        dc_odds = round(1.0 / (p_1x * 1.05), 2)
        if 1.18 <= dc_odds <= 1.40:
            candidates.append({
                "pick": f"{clean_md(f['home_team'])} or Draw (1X)",
                "type": "Double Chance",
                "odds": dc_odds,
                "prob": p_1x
            })

    p_x2 = away_p + draw_p
    if p_x2 >= 0.67:
        dc_odds = round(1.0 / (p_x2 * 1.05), 2)
        if 1.18 <= dc_odds <= 1.40:
            candidates.append({
                "pick": f"Draw or {clean_md(f['away_team'])} (X2)",
                "type": "Double Chance",
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
    best["commence_time"] = f["commence_time"]
    return best

def build_single_slip(candidate_pool: List[Dict[str, Any]], used_fixtures: set) -> Tuple[List[Dict[str, Any]], float]:
    selected_legs = []
    seen_leagues = set()
    total_odds = 1.0

    for leg in candidate_pool:
        if leg["fixture_id"] in used_fixtures:
            continue

        league = leg["league_name"]
        if league in seen_leagues:
            continue

        if (total_odds * leg["odds"]) > 14.5:
            continue

        selected_legs.append(leg)
        seen_leagues.add(league)
        used_fixtures.add(leg["fixture_id"])
        total_odds *= leg["odds"]

        if 10.0 <= total_odds <= 14.0:
            break

    return selected_legs, round(total_odds, 2)

async def build_dual_10_odds_slips_from_db() -> List[str]:
    cached_matches = await load_cached_fixtures()
    if not cached_matches:
        return [
            "⚠️ *Database is empty.*\n\n"
            "Tap **📖 Read Matches** first to ingest current European lines."
        ]

    # Evaluate all matches
    evaluated = []
    for f in cached_matches:
        cand = evaluate_candidate_options(f)
        if cand:
            evaluated.append(cand)

    # Sort descending by calculated probability
    evaluated.sort(key=lambda x: x["prob"], reverse=True)

    used_fixtures = set()

    # Slip A (Primary European Ticket)
    slip_a_legs, slip_a_odds = build_single_slip(evaluated, used_fixtures)

    # Slip B (Secondary Non-Overlapping Ticket)
    slip_b_legs, slip_b_odds = build_single_slip(evaluated, used_fixtures)

    messages = []
    today_str = datetime.now(timezone.utc).strftime("%d %b %Y")

    # Format Ticket A
    if len(slip_a_legs) >= 3 and slip_a_odds >= 7.0:
        card_a = [
            f"🎟️ *EURO TICKET A — (Target ~10 Odds)*",
            f"📅 Date: `{today_str}`",
            f"📈 Total Odds: `{slip_a_odds:.2f}`",
            f"🌍 Distinct Leagues: `{len(slip_a_legs)}`",
            "───────────────────────────\n"
        ]
        for idx, leg in enumerate(slip_a_legs, 1):
            dt_label = leg["commence_time"][:10]
            card_a.append(
                f"*{idx}. {clean_md(leg['fixture'])}* (`{dt_label}`)\n"
                f"🏆 _{clean_md(leg['league_name'])}_\n"
                f"🎲 Odds: `{leg['raw_market']}`\n"
                f"🎯 Pick: *{leg['pick']}* @ `{leg['odds']:.2f}`\n"
                f"🛡️ Safety: `{(leg['prob']*100):.1f}% Confidence`\n"
            )
        card_a.append("───────────────────────────")
        card_a.append("💡 *European Elite Tier. Staking rule: 0.5% – 1.0% unit.*")
        messages.append("\n".join(card_a))
    else:
        messages.append("⚠️ *Ticket A:* Not enough high-probability matches to reach ~10.0 odds safely.")

    # Format Ticket B
    if len(slip_b_legs) >= 3 and slip_b_odds >= 7.0:
        card_b = [
            f"🎟️ *EURO TICKET B — (Target ~10 Odds)*",
            f"📅 Date: `{today_str}`",
            f"📈 Total Odds: `{slip_b_odds:.2f}`",
            f"🌍 Distinct Leagues: `{len(slip_b_legs)}`",
            "───────────────────────────\n"
        ]
        for idx, leg in enumerate(slip_b_legs, 1):
            dt_label = leg["commence_time"][:10]
            card_b.append(
                f"*{idx}. {clean_md(leg['fixture'])}* (`{dt_label}`)\n"
                f"🏆 _{clean_md(leg['league_name'])}_\n"
                f"🎲 Odds: `{leg['raw_market']}`\n"
                f"🎯 Pick: *{leg['pick']}* @ `{leg['odds']:.2f}`\n"
                f"🛡️ Safety: `{(leg['prob']*100):.1f}% Confidence`\n"
            )
        card_b.append("───────────────────────────")
        card_b.append("💡 *Zero overlap with Ticket A. Cross-league diversified.*")
        messages.append("\n".join(card_b))
    else:
        messages.append("ℹ️ *Ticket B:* Need more upcoming fixture dates to generate a completely separate second slip.")

    return messages

# -------------------------------------------------------------------
# Telegram Handlers
# -------------------------------------------------------------------
def build_main_keyboard(cached_count: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton("📖 Read Matches (Store DB)", callback_data="btn_read"),
            InlineKeyboardButton(f"🔮 Predict ({cached_count} Ready)", callback_data="btn_predict")
        ]
    ])

async def start_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await init_db()
    count = await get_cached_fixtures_count()
    await update.message.reply_text(
        "⚽ *European 10-Odds Dual Accumulator Hub*\n\n"
        "• **📖 Read Matches**: Pulls live lines across UEFA cups and top European leagues for the next 7 days and indexes them into SQLite.\n"
        "• **🔮 Predict**: Generates **2 completely independent 10-odds accumulator slips** with zero overlapping fixtures.",
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
            text="⏳ *Pulling UEFA and European league fixtures for the next 7 days into SQLite...*",
            parse_mode="Markdown"
        )
        res = await run_read_and_store_pipeline()
        await status.delete()

        count = await get_cached_fixtures_count()
        if res.get("success"):
            text = (
                f"✅ *European Fixtures Synchronized!*\n\n"
                f"Stored `{res['count']}` matches across `{res['leagues']}` European competitions into SQLite.\n\n"
                f"Tap **🔮 Predict** to generate your 2 accumulator tickets."
            )
        else:
            text = f"⚠️ *Update Failed:* {res.get('message')}"

        await context.bot.send_message(chat_id=chat_id, text=text, parse_mode="Markdown", reply_markup=build_main_keyboard(count))

    elif data == "btn_predict":
        count = await get_cached_fixtures_count()
        if count == 0:
            await context.bot.send_message(
                chat_id=chat_id,
                text="⚠️ Database is empty. Tap **📖 Read Matches** first.",
                parse_mode="Markdown",
                reply_markup=build_main_keyboard(0)
            )
            return

        status = await context.bot.send_message(
            chat_id=chat_id,
            text="⚙️ *Constructing 2 separate 10-odds European accumulators...*",
            parse_mode="Markdown"
        )
        reports = await build_dual_10_odds_slips_from_db()
        await status.delete()

        for report_text in reports:
            await context.bot.send_message(chat_id=chat_id, text=report_text, parse_mode="Markdown")

        # Refresh keyboard status
        await context.bot.send_message(
            chat_id=chat_id,
            text="✅ *Slips generated.* Tap below whenever you want to re-run or refresh:",
            parse_mode="Markdown",
            reply_markup=build_main_keyboard(count)
        )

# -------------------------------------------------------------------
# Entry Point
# -------------------------------------------------------------------
def main():
    if not TELEGRAM_TOKEN:
        raise ValueError("TELEGRAM_BOT_TOKEN environment variable is missing!")

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

    print("🚀 Bot active with European 7-day multi-cup coverage & dual 10-odds slips...")
    app.run_polling(drop_pending_updates=True)

if __name__ == "__main__":
    main()
