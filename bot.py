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

GLOBAL_SOCCER_LEAGUES = {
    "soccer_epl": "Premier League",
    "soccer_efl_champ": "EFL Championship",
    "soccer_uefa_champs_league": "Champions League",
    "soccer_spain_la_liga": "La Liga",
    "soccer_germany_bundesliga": "Bundesliga",
    "soccer_italy_serie_a": "Serie A",
    "soccer_france_ligue_one": "Ligue 1",
    "soccer_netherlands_eredivisie": "Eredivisie",
    "soccer_brazil_campeonato": "Brasil Série A",
    "soccer_argentina_primera_division": "Primera División",
    "soccer_mexico_ligamx": "Liga MX",
    "soccer_usa_mls": "MLS",
    "soccer_japan_j_league": "J-League",
    "soccer_norway_eliteserien": "Eliteserien",
    "soccer_sweden_allsvenskan": "Allsvenskan"
}

def clean_md(text: str) -> str:
    if not text:
        return ""
    for char in ["_", "*", "`", "[", "]", "(", ")"]:
        text = text.replace(char, " ")
    return " ".join(text.split())

# -------------------------------------------------------------------
# Math & Probability Engine
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
# Database Architecture (Persistent Cache)
# -------------------------------------------------------------------
async def init_db():
    async with aiosqlite.connect(DB_NAME) as db:
        # Table 1: Daily Matches Cache
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
        # Table 2: Generated Accumulators Log
        await db.execute("""
            CREATE TABLE IF NOT EXISTS accumulator_slips (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                created_date TEXT,
                total_odds REAL,
                confidence_prob REAL,
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
        async with db.execute(
            "SELECT COUNT(*) FROM active_fixtures WHERE fetch_date = ?", (today_str,)
        ) as cursor:
            row = await cursor.fetchone()
            return row[0] if row else 0

async def load_cached_fixtures() -> List[Dict[str, Any]]:
    today_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    async with aiosqlite.connect(DB_NAME) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute(
            "SELECT * FROM active_fixtures WHERE fetch_date = ?", (today_str,)
        ) as cursor:
            rows = await cursor.fetchall()
            return [dict(r) for r in rows]

# -------------------------------------------------------------------
# Module 1: READ (API Fetcher & DB Ingestion)
# -------------------------------------------------------------------
async def run_read_and_store_pipeline() -> Dict[str, Any]:
    if not ODDS_API_KEY or len(ODDS_API_KEY) < 10:
        return {"success": False, "message": "ODDS_API_KEY is missing in your .env file."}

    now_utc = datetime.now(timezone.utc)
    window_start = now_utc - timedelta(hours=1)
    window_end = now_utc + timedelta(hours=36)

    normalized_fixtures = []

    async with aiohttp.ClientSession() as session:
        sem = asyncio.Semaphore(4)

        async def fetch_league(sport_key: str, label: str):
            async with sem:
                url = f"{BASE_URL}/{sport_key}/odds/"
                params = {
                    "apiKey": ODDS_API_KEY,
                    "regions": "eu,uk,us",
                    "markets": "h2h",
                    "oddsFormat": "decimal"
                }
                try:
                    async with session.get(url, params=params, timeout=10) as resp:
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
                                        "draw_odds": float(draw_o) if draw_o else 3.20,
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
        return {
            "success": True, 
            "count": len(normalized_fixtures), 
            "leagues": len(GLOBAL_SOCCER_LEAGUES)
        }

    return {"success": False, "message": "Zero active fixtures retrieved. Quota limit may be reached."}

# -------------------------------------------------------------------
# Module 2: PREDICT (Offline Generation of 10-Odds Accumulator)
# -------------------------------------------------------------------
def evaluate_fixture_prediction(f: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    prices = [f["home_odds"], f["draw_odds"], f["away_odds"]]
    probs = devig_power_method(prices)
    if len(probs) < 3:
        return None

    home_p, draw_p, away_p = probs[0], probs[1], probs[2]
    candidates = []

    # 1. Straight Win Options (Safe Zone)
    if home_p >= 0.68 and 1.25 <= f["home_odds"] <= 1.58:
        candidates.append({
            "pick": f"{clean_md(f['home_team'])} to Win",
            "market_type": "Home Win",
            "odds": f["home_odds"],
            "prob": home_p
        })
    elif away_p >= 0.68 and 1.25 <= f["away_odds"] <= 1.58:
        candidates.append({
            "pick": f"{clean_md(f['away_team'])} to Win",
            "market_type": "Away Win",
            "odds": f["away_odds"],
            "prob": away_p
        })

    # 2. Double Chance Options (High Stability Zone)
    p_1x = home_p + draw_p
    if p_1x >= 0.73:
        fair_1x_odds = round(1.0 / (p_1x * 1.05), 2)
        if 1.20 <= fair_1x_odds <= 1.48:
            candidates.append({
                "pick": f"{clean_md(f['home_team'])} or Draw (1X)",
                "market_type": "Double Chance 1X",
                "odds": fair_1x_odds,
                "prob": p_1x
            })

    p_x2 = away_p + draw_p
    if p_x2 >= 0.73:
        fair_x2_odds = round(1.0 / (p_x2 * 1.05), 2)
        if 1.20 <= fair_x2_odds <= 1.48:
            candidates.append({
                "pick": f"{clean_md(f['away_team'])} or Draw (X2)",
                "market_type": "Double Chance X2",
                "odds": fair_x2_odds,
                "prob": p_x2
            })

    if not candidates:
        return None

    candidates.sort(key=lambda x: x["prob"], reverse=True)
    best = candidates[0]
    best["fixture"] = f"{f['home_team']} vs {f['away_team']}"
    best["league_name"] = f["league_name"]
    best["market_odds_raw"] = f"H: {f['home_odds']} | D: {f['draw_odds']} | A: {f['away_odds']}"
    return best

async def build_10_odds_slip_from_db() -> str:
    cached_matches = await load_cached_fixtures()
    if not cached_matches:
        return (
            "⚠️ *No cached fixtures found in SQLite for today.*\n\n"
            "Tap **📖 Read Matches** first to ingest today's fixtures and save your API quota."
        )

    evaluated_picks = []
    for f in cached_matches:
        pick = evaluate_fixture_prediction(f)
        if pick:
            evaluated_picks.append(pick)

    # Rank by statistical confidence
    evaluated_picks.sort(key=lambda x: x["prob"], reverse=True)

    selected_legs = []
    seen_leagues = set()
    accumulated_odds = 1.0
    combined_prob = 1.0
    target_odds = 10.0

    for leg in evaluated_picks:
        league = leg["league_name"]
        if league in seen_leagues:
            continue  # Enforce 1 selection per league to eliminate correlated failure

        if accumulated_odds * leg["odds"] > 13.0:
            continue  # Keep target between 10.0x and 12.5x

        selected_legs.append(leg)
        seen_leagues.add(league)
        accumulated_odds *= leg["odds"]
        combined_prob *= leg["prob"]

        if accumulated_odds >= target_odds:
            break

    if accumulated_odds < 8.0 or len(selected_legs) < 4:
        return (
            f"ℹ️ *Insufficient Safety Margin Available*\n\n"
            f"Found {len(selected_legs)} high-probability legs reaching only **{accumulated_odds:.2f}x** odds. "
            f"To protect accuracy, higher-risk picks were rejected. Tap **📖 Read Matches** later when more schedules open."
        )

    today_str = datetime.now(timezone.utc).strftime("%a, %d %b %Y")
    report = [
        f"🎯 *DAILY 10-ODDS MULTI-LEAGUE ACCUMULATOR*",
        f"📅 Date: `{today_str}`",
        f"🌍 Distinct Leagues: `{len(selected_legs)}`",
        f"📈 Total Accumulator Odds: `{accumulated_odds:.2f}`",
        f"🛡️ Estimated Combined Probability: `{(combined_prob * 100):.1f}%`",
        "───────────────────────────\n"
    ]

    for idx, leg in enumerate(selected_legs, 1):
        report.append(
            f"*{idx}. {clean_md(leg['fixture'])}*\n"
            f"🏆 League: _{clean_md(leg['league_name'])}_\n"
            f"🎲 Market Odds: `{leg['market_odds_raw']}`\n"
            f"🎯 Prediction: *{leg['pick']}*\n"
            f"📊 Selection Odds: `{leg['odds']:.2f}` | Confidence: `{(leg['prob'] * 100):.1f}%`\n"
        )

    report.append("───────────────────────────")
    report.append("💡 *Cached locally in SQLite to prevent external API consumption.*")

    # Record slip in history
    async with aiosqlite.connect(DB_NAME) as db:
        await db.execute("""
            INSERT INTO accumulator_slips (created_date, total_odds, confidence_prob, legs_count, legs_summary)
            VALUES (?, ?, ?, ?, ?)
        """, (
            today_str, round(accumulated_odds, 2), round(combined_prob * 100, 1),
            len(selected_legs), json.dumps([l["fixture"] for l in selected_legs])
        ))
        await db.commit()

    return "\n".join(report)

# -------------------------------------------------------------------
# Telegram Interaction & Keyboards
# -------------------------------------------------------------------
def build_main_keyboard(cached_count: int) -> InlineKeyboardMarkup:
    keyboard = [
        [
            InlineKeyboardButton("📖 Read Matches (Store DB)", callback_data="btn_read"),
            InlineKeyboardButton(f"🔮 Predict ({cached_count} Ready)", callback_data="btn_predict")
        ]
    ]
    return InlineKeyboardMarkup(keyboard)

async def start_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await init_db()
    count = await get_cached_fixtures_count()
    await update.message.reply_text(
        "⚽ *Smart Odds Accumulator Engine*\n\n"
        "• **📖 Read Matches**: Fetches live fixtures across global leagues and stores them in SQLite (call once per day).\n"
        "• **🔮 Predict**: Generates a 10.0+ odds multi-league slip entirely from the local database at zero API cost.",
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
            text="⏳ *Reading global leagues and indexing match odds into database...*",
            parse_mode="Markdown"
        )
        res = await run_read_and_store_pipeline()
        await status.delete()

        count = await get_cached_fixtures_count()
        if res.get("success"):
            text = (
                f"✅ *Matches Synchronized!*\n\n"
                f"Successfully pulled and indexed `{res['count']}` fixtures across `{res['leagues']}` global leagues into SQLite.\n\n"
                f"You can now tap **🔮 Predict** at any time without consuming API calls."
            )
        else:
            text = f"⚠️ *Update Failed:* {res.get('message')}"

        await context.bot.send_message(
            chat_id=chat_id,
            text=text,
            parse_mode="Markdown",
            reply_markup=build_main_keyboard(count)
        )

    elif data == "btn_predict":
        count = await get_cached_fixtures_count()
        if count == 0:
            await context.bot.send_message(
                chat_id=chat_id,
                text="⚠️ *Database is empty.* Please click **📖 Read Matches** first.",
                parse_mode="Markdown",
                reply_markup=build_main_keyboard(0)
            )
            return

        status = await context.bot.send_message(
            chat_id=chat_id,
            text="⚙️ *Processing locally cached odds & constructing 10-odds accumulator...*",
            parse_mode="Markdown"
        )
        report = await build_10_odds_slip_from_db()
        await status.delete()

        await context.bot.send_message(
            chat_id=chat_id,
            text=report,
            parse_mode="Markdown",
            reply_markup=build_main_keyboard(count)
        )

# -------------------------------------------------------------------
# Application Entry Point
# -------------------------------------------------------------------
def main():
    if not TELEGRAM_TOKEN:
        raise ValueError("TELEGRAM_BOT_TOKEN environment variable is missing!")

    asyncio.run(init_db())
    app = ApplicationBuilder().token(TELEGRAM_TOKEN).build()
    app.add_handler(CommandHandler("start", start_command))
    app.add_handler(CallbackQueryHandler(button_router))

    print("🚀 Bot running with two-stage Read/Predict caching...")
    app.run_polling()

if __name__ == "__main__":
    main()