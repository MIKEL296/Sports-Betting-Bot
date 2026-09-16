import os
import time
import math
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
            CREATE TABLE IF NOT EXISTS fixtures_48h (
                fixture_id TEXT PRIMARY KEY,
                league_key TEXT,
                league_name TEXT,
                home_team TEXT,
                away_team TEXT,
                home_odds REAL,
                draw_odds REAL,
                away_odds REAL,
                commence_time TEXT,
                fetch_timestamp REAL
            )
        """)
        await db.commit()
    logging.info("SQLite database synchronized.")

async def store_fixtures_to_db(fixtures_data: List[Dict[str, Any]]):
    now_ts = time.time()
    async with aiosqlite.connect(DB_NAME) as db:
        for f in fixtures_data:
            await db.execute("""
                INSERT INTO fixtures_48h (
                    fixture_id, league_key, league_name, home_team, away_team, 
                    home_odds, draw_odds, away_odds, commence_time, fetch_timestamp
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(fixture_id) DO UPDATE SET
                    home_odds=excluded.home_odds,
                    draw_odds=excluded.draw_odds,
                    away_odds=excluded.away_odds,
                    commence_time=excluded.commence_time,
                    fetch_timestamp=excluded.fetch_timestamp
            """, (
                f["id"], f["league_key"], f["league_name"], f["home_team"], f["away_team"],
                f["home_odds"], f["draw_odds"], f["away_odds"], f["commence_time"], now_ts
            ))
        await db.commit()

async def get_cached_fixtures_count() -> int:
    # Only count fixtures whose kickoff is still in the future
    now_iso = datetime.now(timezone.utc).isoformat()
    async with aiosqlite.connect(DB_NAME) as db:
        async with db.execute(
            "SELECT COUNT(*) FROM fixtures_48h WHERE commence_time >= ?", (now_iso,)
        ) as cursor:
            row = await cursor.fetchone()
            return row[0] if row else 0

async def load_future_fixtures() -> List[Dict[str, Any]]:
    now_iso = datetime.now(timezone.utc).isoformat()
    async with aiosqlite.connect(DB_NAME) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute(
            "SELECT * FROM fixtures_48h WHERE commence_time >= ? ORDER BY commence_time ASC", (now_iso,)
        ) as cursor:
            rows = await cursor.fetchall()
            return [dict(r) for r in rows]

# -------------------------------------------------------------------
# Module 1: READ (48-Hour Rolling Window across All Active Leagues)
# -------------------------------------------------------------------
async def run_read_and_store_pipeline() -> Dict[str, Any]:
    if not ODDS_API_KEY:
        return {"success": False, "message": "ODDS_API_KEY missing in .env"}

    active_leagues = {}
    normalized_fixtures = []

    now_utc = datetime.now(timezone.utc)
    # Strict 48-hour window (today + tomorrow)
    window_start = now_utc - timedelta(hours=1)
    window_end = now_utc + timedelta(hours=48)

    async with aiohttp.ClientSession() as session:
        # 1. Discover all active soccer competitions
        try:
            sports_url = f"{BASE_URL}?apiKey={ODDS_API_KEY}"
            async with session.get(sports_url, timeout=12) as s_resp:
                if s_resp.status == 200:
                    sports_data = await s_resp.json()
                    for item in sports_data:
                        if item.get("key", "").startswith("soccer_") and item.get("active", False):
                            active_leagues[item["key"]] = item.get("title", item["key"])
        except Exception as e:
            logging.warning(f"Error querying active sports list: {e}")

        if not active_leagues:
            return {"success": False, "message": "Could not locate active soccer leagues on API."}

        # 2. Concurrently fetch fixtures
        sem = asyncio.Semaphore(6)

        async def fetch_league_matches(sport_key: str, label: str):
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
                except Exception:
                    pass
                return []

        tasks = [fetch_league_matches(k, v) for k, v in active_leagues.items()]
        batch_results = await asyncio.gather(*tasks)
        for res in batch_results:
            normalized_fixtures.extend(res)

    if normalized_fixtures:
        await store_fixtures_to_db(normalized_fixtures)
        return {
            "success": True, 
            "count": len(normalized_fixtures), 
            "leagues": len(active_leagues)
        }

    return {"success": False, "message": "Zero active fixtures found within 48 hours."}

# -------------------------------------------------------------------
# Module 2: PREDICT (Dynamic Engine for 5x, 10x, and 20x Odds)
# -------------------------------------------------------------------
def evaluate_candidate(f: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    prices = [f["home_odds"], f["draw_odds"], f["away_odds"]]
    probs = devig_power_method(prices)
    if len(probs) < 3:
        return None

    home_p, draw_p, away_p = probs[0], probs[1], probs[2]
    candidates = []

    # 1. Double Chance (Primary resilience: 1.18 to 1.36 odds, Prob >= 64%)
    p_1x = home_p + draw_p
    if p_1x >= 0.64:
        dc_odds = round(1.0 / (p_1x * 1.05), 2)
        if 1.16 <= dc_odds <= 1.36:
            candidates.append({
                "pick": f"{clean_md(f['home_team'])} or Draw (1X)",
                "market": "Double Chance",
                "odds": dc_odds,
                "prob": p_1x
            })

    p_x2 = away_p + draw_p
    if p_x2 >= 0.64:
        dc_odds = round(1.0 / (p_x2 * 1.05), 2)
        if 1.16 <= dc_odds <= 1.36:
            candidates.append({
                "pick": f"Draw or {clean_md(f['away_team'])} (X2)",
                "market": "Double Chance",
                "odds": dc_odds,
                "prob": p_x2
            })

    # 2. Outright Win Favorites (Only if prob >= 58%, Odds: 1.25 to 1.62)
    if home_p >= 0.58 and 1.25 <= f["home_odds"] <= 1.62:
        candidates.append({
            "pick": f"{clean_md(f['home_team'])} Win",
            "market": "1X2",
            "odds": f["home_odds"],
            "prob": home_p
        })
    elif away_p >= 0.58 and 1.25 <= f["away_odds"] <= 1.62:
        candidates.append({
            "pick": f"{clean_md(f['away_team'])} Win",
            "market": "1X2",
            "odds": f["away_odds"],
            "prob": away_p
        })

    if not candidates:
        return None

    candidates.sort(key=lambda x: x["prob"], reverse=True)
    best = candidates[0]
    best["fixture_id"] = f["fixture_id"]
    best["fixture"] = f"{f['home_team']} vs {f['away_team']}"
    best["league_name"] = f["league_name"]

    try:
        dt = datetime.fromisoformat(f["commence_time"].replace('Z', '+00:00'))
        best["kickoff"] = dt.strftime("%a %H:%M UTC")
    except Exception:
        best["kickoff"] = "Upcoming"

    return best

async def generate_dynamic_slip(target_odds: float, max_odds: float) -> str:
    cached_matches = await load_future_fixtures()
    if not cached_matches:
        return (
            "⚠️ *Database empty for the next 48 hours.*\n\n"
            "Tap **📖 Read 48h Matches (Store DB)** to pull upcoming fixtures."
        )

    evaluated = []
    for f in cached_matches:
        cand = evaluate_candidate(f)
        if cand:
            evaluated.append(cand)

    # Rank by statistical confidence
    evaluated.sort(key=lambda x: x["prob"], reverse=True)

    selected_legs = []
    league_counts = {}
    current_odds = 1.0

    for leg in evaluated:
        l_name = leg["league_name"]
        # Allow maximum 2 picks per competition to diversify risk
        if league_counts.get(l_name, 0) >= 2:
            continue

        if (current_odds * leg["odds"]) > (max_odds * 1.08):
            continue

        selected_legs.append(leg)
        league_counts[l_name] = league_counts.get(l_name, 0) + 1
        current_odds *= leg["odds"]

        if current_odds >= target_odds:
            break

    if current_odds < (target_odds * 0.75):
        return (
            f"⚠️ *Insufficient Safe Matches Available*\n\n"
            f"Selected {len(selected_legs)} high-probability legs reaching only **{current_odds:.2f}x** odds.\n"
            f"Long-shots were rejected to preserve win rate. Tap **📖 Read 48h Matches** later as bookmakers post new lines."
        )

    target_label = int(target_odds)
    report = [
        f"🎯 *DYNAMIC {target_label} ODDS ACCUMULATOR*",
        f"⏱️ Window: `Today & Tomorrow (48 Hours)`",
        f"📈 Total Accumulator Odds: `{current_odds:.2f}`",
        f"🔒 Total Selections: `{len(selected_legs)} Matches`",
        "───────────────────────────\n"
    ]

    for idx, leg in enumerate(selected_legs, 1):
        report.append(
            f"*{idx}. {clean_md(leg['fixture'])}* (`{leg['kickoff']}`)\n"
            f"🏆 _{clean_md(leg['league_name'])}_\n"
            f"🎯 Pick: *{leg['pick']}* @ `{leg['odds']:.2f}`\n"
            f"🛡️ Safety: `{(leg['prob']*100):.1f}% Confidence`\n"
        )

    report.append("───────────────────────────")
    report.append(f"💡 *Generated from local SQLite cache at zero API cost.*")
    return "\n".join(report)

# -------------------------------------------------------------------
# Telegram Keyboards & Router
# -------------------------------------------------------------------
def build_main_keyboard(cached_count: int) -> InlineKeyboardMarkup:
    keyboard = [
        [InlineKeyboardButton("📖 Read 48h Matches (Store DB)", callback_data="btn_read")],
        [
            InlineKeyboardButton("🎯 5 Odds", callback_data="pred_5"),
            InlineKeyboardButton("🔥 10 Odds", callback_data="pred_10"),
            InlineKeyboardButton("🚀 20 Odds", callback_data="pred_20")
        ]
    ]
    return InlineKeyboardMarkup(keyboard)

async def start_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await init_db()
    count = await get_cached_fixtures_count()
    await update.message.reply_text(
        f"⚽ *Smart Accumulator Hub (48-Hour Engine)*\n\n"
        f"📊 *Cached Matches Available:* `{count}`\n\n"
        f"• **📖 Read 48h Matches**: Ingests all games kicking off today and tomorrow into SQLite.\n"
        f"• **Choose your odds**: Tap **5 Odds**, **10 Odds**, or **20 Odds** below to build your preferred ticket dynamically:",
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
            text="⏳ *Reading all matches scheduled for today & tomorrow (48 hours) into SQLite...*",
            parse_mode="Markdown"
        )
        res = await run_read_and_store_pipeline()
        await status.delete()

        count = await get_cached_fixtures_count()
        if res.get("success"):
            text = (
                f"✅ *48-Hour Slate Synchronized!*\n\n"
                f"Stored `{res['count']}` matches across `{res['leagues']}` competitions into SQLite.\n\n"
                f"Choose your ticket multiplier below:"
            )
        else:
            text = f"⚠️ *Update Notice:* {res.get('message')}"

        await context.bot.send_message(chat_id=chat_id, text=text, parse_mode="Markdown", reply_markup=build_main_keyboard(count))

    elif data in ["pred_5", "pred_10", "pred_20"]:
        count = await get_cached_fixtures_count()
        if count == 0:
            await context.bot.send_message(
                chat_id=chat_id,
                text="⚠️ *No matches cached.* Tap **📖 Read 48h Matches (Store DB)** first.",
                parse_mode="Markdown",
                reply_markup=build_main_keyboard(0)
            )
            return

        target_map = {
            "pred_5": (5.0, 7.0),
            "pred_10": (10.0, 13.5),
            "pred_20": (20.0, 26.0)
        }
        target, maximum = target_map[data]

        status = await context.bot.send_message(
            chat_id=chat_id,
            text=f"⚙️ *Compounding safest multi-league selections for ~{int(target)} odds ticket...*",
            parse_mode="Markdown"
        )
        report = await generate_dynamic_slip(target_odds=target, max_odds=maximum)
        await status.delete()

        await context.bot.send_message(
            chat_id=chat_id,
            text=report,
            parse_mode="Markdown",
            reply_markup=build_main_keyboard(count)
        )

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

    print("🚀 48-Hour Bot active with 5x / 10x / 20x dynamic odds selector...")
    app.run_polling(drop_pending_updates=True)

if __name__ == "__main__":
    main()
