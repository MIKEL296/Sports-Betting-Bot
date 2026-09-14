import os
import time
import math
import json
import logging
import aiohttp
import asyncio
import aiosqlite
from typing import List, Dict, Any, Optional
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

load_dotenv()

TELEGRAM_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
ODDS_API_KEY = os.getenv("ODDS_API_KEY")
BASE_URL = "https://api.the-odds-api.com/v4/sports"
DB_NAME = "todays_predictions.db"

logging.basicConfig(format="%(asctime)s - %(name)s - %(levelname)s - %(message)s", level=logging.INFO)

# Comprehensive global league coverage
GLOBAL_SOCCER_LEAGUES = {
    "soccer_epl": "Premier League (ENG)",
    "soccer_efl_champ": "Championship (ENG)",
    "soccer_england_league1": "League One (ENG)",
    "soccer_uefa_champs_league": "Champions League",
    "soccer_uefa_europa_league": "Europa League",
    "soccer_spain_la_liga": "La Liga (ESP)",
    "soccer_germany_bundesliga": "Bundesliga (GER)",
    "soccer_italy_serie_a": "Serie A (ITA)",
    "soccer_france_ligue_one": "Ligue 1 (FRA)",
    "soccer_netherlands_eredivisie": "Eredivisie (NED)",
    "soccer_portugal_primeira_liga": "Primeira Liga (POR)",
    "soccer_turkey_super_league": "Super Lig (TUR)",
    "soccer_belgium_first_div": "Pro League (BEL)",
    "soccer_brazil_campeonato": "Série A (BRA)",
    "soccer_argentina_primera_division": "Primera (ARG)",
    "soccer_usa_mls": "MLS (USA)",
    "soccer_japan_j_league": "J-League (JPN)",
    "soccer_norway_eliteserien": "Eliteserien (NOR)",
    "soccer_sweden_allsvenskan": "Allsvenskan (SWE)"
}

def clean_md(text: str) -> str:
    if not text:
        return ""
    for char in ["_", "*", "`", "[", "]", "(", ")"]:
        text = text.replace(char, " ")
    return " ".join(text.split())

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
# Database Engine
# -------------------------------------------------------------------
async def init_db():
    async with aiosqlite.connect(DB_NAME) as db:
        await db.execute("""
            CREATE TABLE IF NOT EXISTS active_fixtures (
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
                total_odds REAL,
                legs_count INTEGER,
                legs_summary TEXT,
                timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
            )
        """)
        await db.commit()

async def store_fixtures_to_db(fixtures_data: List[Dict[str, Any]]):
    today_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    async with aiosqlite.connect(DB_NAME) as db:
        for f in fixtures_data:
            await db.execute("""
                INSERT INTO active_fixtures (
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
        async with db.execute("SELECT COUNT(*) FROM active_fixtures WHERE fetch_date = ?", (today_str,)) as cursor:
            row = await cursor.fetchone()
            return row[0] if row else 0

async def load_cached_fixtures() -> List[Dict[str, Any]]:
    today_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    async with aiosqlite.connect(DB_NAME) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute("SELECT * FROM active_fixtures WHERE fetch_date = ?", (today_str,)) as cursor:
            rows = await cursor.fetchall()
            return [dict(r) for r in rows]

# -------------------------------------------------------------------
# Module 1: READ & STORE (Ingest across leagues)
# -------------------------------------------------------------------
async def run_read_and_store_pipeline() -> Dict[str, Any]:
    if not ODDS_API_KEY:
        return {"success": False, "message": "ODDS_API_KEY missing in .env"}

    now_utc = datetime.now(timezone.utc)
    # Scan up to 4 days ahead to catch full weekend/weekday rounds
    window_start = now_utc - timedelta(hours=1)
    window_end = now_utc + timedelta(hours=96)

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
                    logging.warning(f"Error reading {label}: {e}")
                return []

        tasks = [fetch_league(k, v) for k, v in GLOBAL_SOCCER_LEAGUES.items()]
        batch_results = await asyncio.gather(*tasks)
        for res in batch_results:
            normalized_fixtures.extend(res)

    if normalized_fixtures:
        await store_fixtures_to_db(normalized_fixtures)
        return {"success": True, "count": len(normalized_fixtures), "leagues": len(GLOBAL_SOCCER_LEAGUES)}

    return {"success": False, "message": "Zero matches returned. Verify your API Key and active lines."}

# -------------------------------------------------------------------
# Module 2: PREDICT (Accurate 10-12 Odds SportyBet-Style Accumulator)
# -------------------------------------------------------------------
def evaluate_fixture_candidate(f: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    prices = [f["home_odds"], f["draw_odds"], f["away_odds"]]
    probs = devig_power_method(prices)
    if len(probs) < 3:
        return None

    home_p, draw_p, away_p = probs[0], probs[1], probs[2]
    candidates = []

    # 1. Straight 1X2 Favorites (1.30 to 1.65)
    if home_p >= 0.58 and 1.28 <= f["home_odds"] <= 1.65:
        candidates.append({
            "pick": f"{clean_md(f['home_team'])} (Home Win)",
            "type": "1X2",
            "odds": f["home_odds"],
            "prob": home_p
        })
    elif away_p >= 0.58 and 1.28 <= f["away_odds"] <= 1.65:
        candidates.append({
            "pick": f"{clean_md(f['away_team'])} (Away Win)",
            "type": "1X2",
            "odds": f["away_odds"],
            "prob": away_p
        })

    # 2. Solid Double Chance (1X / X2) (1.18 to 1.38)
    p_1x = home_p + draw_p
    if p_1x >= 0.68:
        # Standard double chance market price conversion with 6% margin
        dc_odds = round(1.0 / (p_1x * 1.06), 2)
        if 1.18 <= dc_odds <= 1.38:
            candidates.append({
                "pick": f"{clean_md(f['home_team'])} or Draw (1X)",
                "type": "Double Chance",
                "odds": dc_odds,
                "prob": p_1x
            })

    p_x2 = away_p + draw_p
    if p_x2 >= 0.68:
        dc_odds = round(1.0 / (p_x2 * 1.06), 2)
        if 1.18 <= dc_odds <= 1.38:
            candidates.append({
                "pick": f"Draw or {clean_md(f['away_team'])} (X2)",
                "type": "Double Chance",
                "odds": dc_odds,
                "prob": p_x2
            })

    if not candidates:
        return None

    # Pick the option providing the highest safety margin
    candidates.sort(key=lambda x: x["prob"], reverse=True)
    best = candidates[0]
    best["fixture"] = f"{f['home_team']} vs {f['away_team']}"
    best["league_name"] = f["league_name"]
    best["raw_market"] = f"H: {f['home_odds']} | D: {f['draw_odds']} | A: {f['away_odds']}"
    return best

async def build_10_odds_slip_from_db() -> str:
    cached_matches = await load_cached_fixtures()
    if not cached_matches:
        return "⚠️ *Database is empty.* Click **📖 Read Matches** first."

    evaluated = []
    for f in cached_matches:
        cand = evaluate_fixture_candidate(f)
        if cand:
            evaluated.append(cand)

    # Rank all available plays by probability
    evaluated.sort(key=lambda x: x["prob"], reverse=True)

    selected_legs = []
    seen_leagues = set()
    total_odds = 1.0

    # Compound until odds reach the 10.0 - 13.0 target
    for leg in evaluated:
        league = leg["league_name"]
        if league in seen_leagues:
            continue  # Keep strict 1 match per league diversity

        if (total_odds * leg["odds"]) > 14.5:
            continue

        selected_legs.append(leg)
        seen_leagues.add(league)
        total_odds *= leg["odds"]

        if 10.0 <= total_odds <= 14.0:
            break

    if total_odds < 9.0:
        return (
            f"⚠️ *Not Enough Qualified Games Yet*\n\n"
            f"Selected {len(selected_legs)} high-probability matches yielding **{total_odds:.2f}x** odds.\n"
            f"To keep win probability high, risky long-shots were not added. "
            f"Tap **📖 Read Matches** when additional leagues update their fixture lines."
        )

    today_str = datetime.now(timezone.utc).strftime("%d %b %Y")
    report = [
        f"🎟️ *10+ ODDS MULTI-LEAGUE ACCUMULATOR*",
        f"📅 Date: `{today_str}`",
        f"📈 Combined Odds: `{total_odds:.2f}`",
        f"🌍 Total Leagues: `{len(selected_legs)}`",
        "───────────────────────────\n"
    ]

    for idx, leg in enumerate(selected_legs, 1):
        report.append(
            f"*{idx}. {clean_md(leg['fixture'])}*\n"
            f"🏆 _{clean_md(leg['league_name'])}_\n"
            f"🎲 Market Odds: `{leg['raw_market']}`\n"
            f"🎯 *Pick:* `{leg['pick']}` @ *{leg['odds']:.2f}*\n"
            f"🛡️ Safety: `{(leg['prob']*100):.1f}% Confidence`\n"
        )

    report.append("───────────────────────────")
    report.append("💡 *Cross-league diversification active. No single league carries duplicate exposure.*")

    # Record ticket to database
    async with aiosqlite.connect(DB_NAME) as db:
        await db.execute("""
            INSERT INTO accumulator_slips (created_date, total_odds, legs_count, legs_summary)
            VALUES (?, ?, ?, ?)
        """, (today_str, round(total_odds, 2), len(selected_legs), json.dumps([l["fixture"] for l in selected_legs])))
        await db.commit()

    return "\n".join(report)

# -------------------------------------------------------------------
# Telegram Interaction & Setup
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
        "⚽ *Smart 10-Odds Accumulator Bot*\n\n"
        "• **📖 Read Matches**: Pulls upcoming matches across 19 global leagues and caches them in SQLite.\n"
        "• **🔮 Predict**: Generates an optimized 10+ odds accumulator ticket locally without consuming API calls.",
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
            text="⏳ *Ingesting global leagues and indexing match odds into SQLite...*",
            parse_mode="Markdown"
        )
        res = await run_read_and_store_pipeline()
        await status.delete()

        count = await get_cached_fixtures_count()
        if res.get("success"):
            text = (
                f"✅ *Matches Synchronized!*\n\n"
                f"Stored `{res['count']}` fixtures across `{res['leagues']}` leagues into SQLite.\n"
                f"Tap **🔮 Predict** to construct your 10-odds slip."
            )
        else:
            text = f"⚠️ *Fetch Issue:* {res.get('message')}"

        await context.bot.send_message(chat_id=chat_id, text=text, parse_mode="Markdown", reply_markup=build_main_keyboard(count))

    elif data == "btn_predict":
        count = await get_cached_fixtures_count()
        if count == 0:
            await context.bot.send_message(
                chat_id=chat_id,
                text="⚠️ Database is empty. Please tap **📖 Read Matches** first.",
                parse_mode="Markdown",
                reply_markup=build_main_keyboard(0)
            )
            return

        status = await context.bot.send_message(
            chat_id=chat_id,
            text="⚙️ *Generating 10.0+ odds multi-league slip from cached data...*",
            parse_mode="Markdown"
        )
        report = await build_10_odds_slip_from_db()
        await status.delete()

        await context.bot.send_message(chat_id=chat_id, text=report, parse_mode="Markdown", reply_markup=build_main_keyboard(count))

def main():
    if not TELEGRAM_TOKEN:
        raise ValueError("TELEGRAM_BOT_TOKEN is missing!")

    asyncio.run(init_db())

    # Generous connection parameters to eliminate TimeOut errors
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

    print("🚀 Bot active with timeout resilience and SportyBet-style accumulator engine...")
    app.run_polling(drop_pending_updates=True)

if __name__ == "__main__":
    main()
