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
# Database Engine
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
    logging.info("SQLite storage initialized.")

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
# Module 1: READ (Auto-discovers ALL Active Competitions Worldwide)
# -------------------------------------------------------------------
async def run_read_and_store_pipeline() -> Dict[str, Any]:
    if not ODDS_API_KEY:
        return {"success": False, "message": "ODDS_API_KEY missing in .env"}

    active_soccer_leagues = {}
    normalized_fixtures = []

    now_utc = datetime.now(timezone.utc)
    window_start = now_utc - timedelta(hours=1)
    window_end = now_utc + timedelta(hours=30)  # Covers full 24h cycle regardless of timezone

    async with aiohttp.ClientSession() as session:
        # Step 1: Ingest active competitions dynamically from the sports endpoint
        try:
            sports_url = f"{BASE_URL}?apiKey={ODDS_API_KEY}"
            async with session.get(sports_url, timeout=12) as s_resp:
                if s_resp.status == 200:
                    sports_data = await s_resp.json()
                    for item in sports_data:
                        # Grab any soccer competition with live active lines
                        if item.get("key", "").startswith("soccer_") and item.get("active", False):
                            active_soccer_leagues[item["key"]] = item.get("title", item["key"])
        except Exception as e:
            logging.warning(f"Error querying active sports list: {e}")

        if not active_soccer_leagues:
            return {"success": False, "message": "Could not locate active soccer leagues on API."}

        # Step 2: Concurrently pull fixtures across all discovered active competitions
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

        tasks = [fetch_league_matches(k, v) for k, v in active_soccer_leagues.items()]
        batch_results = await asyncio.gather(*tasks)
        for res in batch_results:
            normalized_fixtures.extend(res)

    if normalized_fixtures:
        await store_fixtures_to_db(normalized_fixtures)
        return {
            "success": True, 
            "count": len(normalized_fixtures), 
            "leagues": len(active_soccer_leagues)
        }

    return {"success": False, "message": "Zero live matches found in current 24-hour window."}

# -------------------------------------------------------------------
# Module 2: PREDICT (Build 2 Separate 5.0x SportyBet-Style Slips)
# -------------------------------------------------------------------
def evaluate_match_candidate(f: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    prices = [f["home_odds"], f["draw_odds"], f["away_odds"]]
    probs = devig_power_method(prices)
    if len(probs) < 3:
        return None

    home_p, draw_p, away_p = probs[0], probs[1], probs[2]
    candidates = []

    # 1. Double Chance (Primary high-safety market: 1.18 to 1.35 odds)
    p_1x = home_p + draw_p
    if p_1x >= 0.65:
        dc_odds = round(1.0 / (p_1x * 1.05), 2)
        if 1.17 <= dc_odds <= 1.35:
            candidates.append({
                "pick": f"{clean_md(f['home_team'])} or Draw (1X)",
                "market": "Double Chance",
                "odds": dc_odds,
                "prob": p_1x
            })

    p_x2 = away_p + draw_p
    if p_x2 >= 0.65:
        dc_odds = round(1.0 / (p_x2 * 1.05), 2)
        if 1.17 <= dc_odds <= 1.35:
            candidates.append({
                "pick": f"Draw or {clean_md(f['away_team'])} (X2)",
                "market": "Double Chance",
                "odds": dc_odds,
                "prob": p_x2
            })

    # 2. Outright Favorite (Only if heavy confidence: 1.25 to 1.60 odds)
    if home_p >= 0.58 and 1.25 <= f["home_odds"] <= 1.60:
        candidates.append({
            "pick": f"{clean_md(f['home_team'])} Win",
            "market": "1X2",
            "odds": f["home_odds"],
            "prob": home_p
        })
    elif away_p >= 0.58 and 1.25 <= f["away_odds"] <= 1.60:
        candidates.append({
            "pick": f"{clean_md(f['away_team'])} Win",
            "market": "1X2",
            "odds": f["away_odds"],
            "prob": away_p
        })

    if not candidates:
        return None

    # Pick the selection providing the best statistical safety margin
    candidates.sort(key=lambda x: x["prob"], reverse=True)
    best = candidates[0]
    best["fixture_id"] = f["fixture_id"]
    best["fixture"] = f"{f['home_team']} vs {f['away_team']}"
    best["league_name"] = f["league_name"]

    try:
        dt = datetime.fromisoformat(f["commence_time"].replace('Z', '+00:00'))
        best["kickoff"] = dt.strftime("%H:%M UTC")
    except Exception:
        best["kickoff"] = "Today"

    return best

def build_single_slip(candidate_pool: List[Dict[str, Any]], used_fixtures: set) -> Tuple[List[Dict[str, Any]], float]:
    selected_legs = []
    league_counts = {}
    total_odds = 1.0

    for leg in candidate_pool:
        if leg["fixture_id"] in used_fixtures:
            continue

        l_name = leg["league_name"]
        # Allow up to 2 distinct matches per competition to leverage busy matchdays
        if league_counts.get(l_name, 0) >= 2:
            continue

        if (total_odds * leg["odds"]) > 7.0:
            continue

        selected_legs.append(leg)
        league_counts[l_name] = league_counts.get(l_name, 0) + 1
        used_fixtures.add(leg["fixture_id"])
        total_odds *= leg["odds"]

        if 5.0 <= total_odds <= 6.8:
            break

    return selected_legs, round(total_odds, 2)

async def build_dual_5_odds_slips() -> List[str]:
    cached_matches = await load_cached_fixtures()
    if not cached_matches:
        return ["⚠️ *Database empty for today.* Tap **📖 Read Today's Matches** first."]

    evaluated = []
    for f in cached_matches:
        cand = evaluate_match_candidate(f)
        if cand:
            evaluated.append(cand)

    # Rank by probability
    evaluated.sort(key=lambda x: x["prob"], reverse=True)

    used_fixtures = set()

    slip_1_legs, slip_1_odds = build_single_slip(evaluated, used_fixtures)
    slip_2_legs, slip_2_odds = build_single_slip(evaluated, used_fixtures)

    messages = []
    today_str = datetime.now(timezone.utc).strftime("%a, %d %b %Y")

    # Format Ticket 1
    if len(slip_1_legs) >= 3 and slip_1_odds >= 3.5:
        card_1 = [
            f"🎯 *SAME-DAY SLIP 1 — (~5.0 ODDS)*",
            f"📅 Date: `{today_str}`",
            f"📈 Total Odds: `{slip_1_odds:.2f}`",
            f"🔒 Legs: `{len(slip_1_legs)} Matches`",
            "───────────────────────────\n"
        ]
        for idx, leg in enumerate(slip_1_legs, 1):
            card_1.append(
                f"*{idx}. {clean_md(leg['fixture'])}* (`{leg['kickoff']}`)\n"
                f"🏆 _{clean_md(leg['league_name'])}_\n"
                f"🎯 *{leg['pick']}* @ `{leg['odds']:.2f}`  ({(leg['prob']*100):.1f}%)\n"
            )
        card_1.append("───────────────────────────")
        card_1.append("⚡ *All selections conclude today for clean cashout.*")
        messages.append("\n".join(card_1))
    else:
        messages.append("⚠️ *Slip 1:* Unable to form ~5.0 odds with today's fixture pool.")

    # Format Ticket 2
    if len(slip_2_legs) >= 3 and slip_2_odds >= 3.5:
        card_2 = [
            f"🎯 *SAME-DAY SLIP 2 — (~5.0 ODDS)*",
            f"📅 Date: `{today_str}`",
            f"📈 Total Odds: `{slip_2_odds:.2f}`",
            f"🔒 Legs: `{len(slip_2_legs)} Matches`",
            "───────────────────────────\n"
        ]
        for idx, leg in enumerate(slip_2_legs, 1):
            card_2.append(
                f"*{idx}. {clean_md(leg['fixture'])}* (`{leg['kickoff']}`)\n"
                f"🏆 _{clean_md(leg['league_name'])}_\n"
                f"🎯 *{leg['pick']}* @ `{leg['odds']:.2f}`  ({(leg['prob']*100):.1f}%)\n"
            )
        card_2.append("───────────────────────────")
        card_2.append("⚡ *Zero overlap with Slip 1.*")
        messages.append("\n".join(card_2))
    else:
        messages.append("ℹ️ *Slip 2:* Need more distinct games scheduled today for the second slip.")

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
        "⚽ *Global Same-Day 5-Odds Accumulator Engine*\n\n"
        "• **📖 Read Today's Matches**: Scans every active soccer competition worldwide for matches kicking off within 24 hours[span_2](start_span)[span_2](end_span)[span_3](start_span)[span_3](end_span).\n"
        "• **🔮 Predict**: Formulates **2 separate ~5.0 odds slips** styled like SportyBet Double Chance multiples[span_4](start_span)[span_4](end_span).",
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
            text="⏳ *Discovering all active global soccer competitions and caching 24h matches...*",
            parse_mode="Markdown"
        )
        res = await run_read_and_store_pipeline()
        await status.delete()

        count = await get_cached_fixtures_count()
        if res.get("success"):
            text = (
                f"✅ *Matches Synchronized!*\n\n"
                f"Indexed `{res['count']}` fixtures across `{res['leagues']}` active competitions into SQLite.\n\n"
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
                text="⚠️ Database empty. Tap **📖 Read Today's Matches** first.",
                parse_mode="Markdown",
                reply_markup=build_main_keyboard(0)
            )
            return

        status = await context.bot.send_message(
            chat_id=chat_id,
            text="⚙️ *Constructing 2 separate 5.0 odds slips...*",
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

    print("🚀 Bot active with full worldwide competition discovery & dual 5-odds builder...")
    app.run_polling(drop_pending_updates=True)

if __name__ == "__main__":
    main()
