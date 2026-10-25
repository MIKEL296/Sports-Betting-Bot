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
DB_NAME = "multisport_predictions.db"

logging.basicConfig(format="%(asctime)s - %(name)s - %(levelname)s - %(message)s", level=logging.INFO)

# Comprehensive Multi-Sport League Mapping
ALL_SUPPORTED_SPORTS = {
    # Basketball
    "basketball_nba": "NBA (Basketball)",
    "basketball_euroleague": "EuroLeague (Basketball)",
    "basketball_ncaab": "NCAAB (Basketball)",
    # Tennis
    "tennis_atp_us_open": "ATP Tennis",
    "tennis_wta_us_open": "WTA Tennis",
    "tennis_atp_aus_open": "ATP Tennis",
    "tennis_wta_aus_open": "WTA Tennis",
    # Ice Hockey
    "icehockey_nhl": "NHL (Ice Hockey)",
    "icehockey_sweden_hockey_league": "SHL (Ice Hockey)",
    # Baseball
    "baseball_mlb": "MLB (Baseball)",
    "baseball_kbo": "KBO (Baseball)",
    # Soccer
    "soccer_epl": "Premier League (Soccer)",
    "soccer_spain_la_liga": "La Liga (Soccer)",
    "soccer_germany_bundesliga": "Bundesliga (Soccer)",
    "soccer_italy_serie_a": "Serie A (Soccer)",
    "soccer_france_ligue_one": "Ligue 1 (Soccer)",
    "soccer_uefa_champs_league": "UEFA Champions League",
    "soccer_uefa_europa_league": "UEFA Europa League",
    "soccer_netherlands_eredivisie": "Eredivisie (Soccer)",
    "soccer_portugal_primeira_liga": "Primeira Liga (Soccer)",
    "soccer_turkey_super_league": "Süper Lig (Soccer)",
    "soccer_brazil_campeonato": "Série A (Soccer)"
}

def clean_md(text: str) -> str:
    if not text:
        return ""
    for char in ["_", "*", "`", "[", "]", "(", ")"]:
        text = text.replace(char, " ")
    return " ".join(text.split())

# -------------------------------------------------------------------
# Probability Devigging Engine
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
# Universal Market Evaluator (Handles 2-way & 3-way sports cleanly)
# -------------------------------------------------------------------
def evaluate_market_across_sports(f: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    try:
        outcomes = eval(f["outcomes_json"])
    except Exception:
        return None

    home_name = clean_md(f["home_team"])
    away_name = clean_md(f["away_team"])
    sport_key = f["sport_key"]

    # --- CASE A: 2-Way Markets (Basketball, Tennis, Hockey, Baseball Moneyline) ---
    if len(outcomes) == 2:
        o1_name, o1_price = outcomes[0]["name"], float(outcomes[0]["price"])
        o2_name, o2_price = outcomes[1]["name"], float(outcomes[1]["price"])
        
        probs = devig_power_method([o1_price, o2_price])
        if len(probs) != 2:
            return None

        p1, p2 = probs[0], probs[1]
        candidates = []

        # Target value odds range (1.50 to 1.95) with strong probability support
        if p1 >= 0.50 and 1.50 <= o1_price <= 1.95:
            candidates.append({
                "pick": f"{clean_md(o1_name)} Win",
                "category": "🎯 Moneyline",
                "odds": o1_price,
                "prob": p1
            })
        if p2 >= 0.50 and 1.50 <= o2_price <= 1.95:
            candidates.append({
                "pick": f"{clean_md(o2_name)} Win",
                "category": "🎯 Moneyline",
                "odds": o2_price,
                "prob": p2
            })

        if not candidates:
            return None

        candidates.sort(key=lambda x: (x["prob"] * x["odds"]), reverse=True)
        best = candidates[0]
        best["fixture_id"] = f["fixture_id"]
        best["fixture"] = f"{home_name} vs {away_name}"
        best["league_name"] = f["sport_name"]

        try:
            dt = datetime.fromisoformat(f["commence_time"].replace('Z', '+00:00'))
            best["kickoff"] = dt.strftime("%a %H:%M UTC")
        except Exception:
            best["kickoff"] = "Upcoming"

        return best

    # --- CASE B: 3-Way Markets (Soccer 1X2) ---
    elif len(outcomes) >= 3:
        home_price = next((float(o["price"]) for o in outcomes if o["name"] == f["home_team"]), None)
        away_price = next((float(o["price"]) for o in outcomes if o["name"] == f["away_team"]), None)
        draw_price = next((float(o["price"]) for o in outcomes if o["name"] == "Draw"), 3.25)

        if not home_price or not away_price:
            return None

        probs = devig_power_method([home_price, draw_price, away_price])
        if len(probs) < 3:
            return None

        home_p, draw_p, away_p = probs[0], probs[1], probs[2]
        candidates = []

        if home_p >= 0.48 and 1.50 <= home_price <= 1.95:
            candidates.append({
                "pick": f"{home_name} to Win",
                "category": "🏆 Soccer 1X2",
                "odds": home_price,
                "prob": home_p
            })
        elif away_p >= 0.48 and 1.50 <= away_price <= 1.95:
            candidates.append({
                "pick": f"{away_name} to Win",
                "category": "🏆 Soccer 1X2",
                "odds": away_price,
                "prob": away_p
            })

        if not candidates:
            return None

        candidates.sort(key=lambda x: (x["prob"] * x["odds"]), reverse=True)
        best = candidates[0]
        best["fixture_id"] = f["fixture_id"]
        best["fixture"] = f"{home_name} vs {away_name}"
        best["league_name"] = f["sport_name"]

        try:
            dt = datetime.fromisoformat(f["commence_time"].replace('Z', '+00:00'))
            best["kickoff"] = dt.strftime("%a %H:%M UTC")
        except Exception:
            best["kickoff"] = "Upcoming"

        return best

    return None

# -------------------------------------------------------------------
# Database Architecture
# -------------------------------------------------------------------
async def init_db():
    async with aiosqlite.connect(DB_NAME) as db:
        await db.execute("""
            CREATE TABLE IF NOT EXISTS universal_fixtures (
                fixture_id TEXT PRIMARY KEY,
                sport_key TEXT,
                sport_name TEXT,
                home_team TEXT,
                away_team TEXT,
                outcomes_json TEXT,
                commence_time TEXT,
                fetch_timestamp REAL
            )
        """)
        await db.commit()

async def store_fixtures_to_db(fixtures_data: List[Dict[str, Any]]):
    now_ts = time.time()
    async with aiosqlite.connect(DB_NAME) as db:
        for f in fixtures_data:
            await db.execute("""
                INSERT INTO universal_fixtures (
                    fixture_id, sport_key, sport_name, home_team, away_team, 
                    outcomes_json, commence_time, fetch_timestamp
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(fixture_id) DO UPDATE SET
                    sport_key=excluded.sport_key,
                    sport_name=excluded.sport_name,
                    home_team=excluded.home_team,
                    away_team=excluded.away_team,
                    outcomes_json=excluded.outcomes_json,
                    commence_time=excluded.commence_time,
                    fetch_timestamp=excluded.fetch_timestamp
            """, (
                f["id"], f["sport_key"], f["sport_name"], f["home_team"], f["away_team"],
                str(f["outcomes"]), f["commence_time"], now_ts
            ))
        await db.commit()

async def get_cached_fixtures_count() -> int:
    now_iso = datetime.now(timezone.utc).isoformat()
    async with aiosqlite.connect(DB_NAME) as db:
        async with db.execute(
            "SELECT COUNT(*) FROM universal_fixtures WHERE commence_time >= ?", (now_iso,)
        ) as cursor:
            row = await cursor.fetchone()
            return row[0] if row else 0

async def load_future_fixtures() -> List[Dict[str, Any]]:
    now_iso = datetime.now(timezone.utc).isoformat()
    async with aiosqlite.connect(DB_NAME) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute(
            "SELECT * FROM universal_fixtures WHERE commence_time >= ? ORDER BY commence_time ASC", (now_iso,)
        ) as cursor:
            rows = await cursor.fetchall()
            return [dict(r) for r in rows]

# -------------------------------------------------------------------
# Module 1: Ingest All Sports from API
# -------------------------------------------------------------------
async def run_read_and_store_pipeline() -> Dict[str, Any]:
    if not ODDS_API_KEY:
        return {"success": False, "message": "ODDS_API_KEY missing in .env"}

    active_sports = {}
    normalized_fixtures = []

    now_utc = datetime.now(timezone.utc)
    window_start = now_utc - timedelta(hours=1)
    window_end = now_utc + timedelta(hours=72)

    async with aiohttp.ClientSession() as session:
        try:
            sports_url = f"{BASE_URL}?apiKey={ODDS_API_KEY}"
            async with session.get(sports_url, timeout=12) as s_resp:
                if s_resp.status == 200:
                    sports_data = await s_resp.json()
                    for item in sports_data:
                        key = item.get("key", "")
                        # Explicitly grab basketball, tennis, ice hockey, baseball, and soccer
                        if any(k in key for k in ["basketball_", "tennis_", "icehockey_", "baseball_", "soccer_"]) and item.get("active", False):
                            active_sports[key] = item.get("title", key)
        except Exception as e:
            logging.warning(f"Error querying active sports list: {e}")

        combined_pool = {**ALL_SUPPORTED_SPORTS, **active_sports}
        sem = asyncio.Semaphore(8)

        async def fetch_sport_matches(sport_key: str, label: str):
            async with sem:
                url = f"{BASE_URL}/{sport_key}/odds/"
                params = {
                    "apiKey": ODDS_API_KEY,
                    "regions": "eu,uk,us",
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

                                if outcomes and len(outcomes) >= 2:
                                    results.append({
                                        "id": fixture["id"],
                                        "sport_key": sport_key,
                                        "sport_name": label,
                                        "home_team": home_name,
                                        "away_team": away_name,
                                        "outcomes": outcomes,
                                        "commence_time": commence_raw
                                    })
                            return results
                except Exception:
                    pass
                return []

        tasks = [fetch_sport_matches(k, v) for k, v in combined_pool.items()]
        batch_results = await asyncio.gather(*tasks)
        for res in batch_results:
            normalized_fixtures.extend(res)

    if normalized_fixtures:
        await store_fixtures_to_db(normalized_fixtures)
        return {
            "success": True, 
            "count": len(normalized_fixtures), 
            "sports": len(combined_pool)
        }

    return {"success": False, "message": "Zero active fixtures found across sports pool."}

# -------------------------------------------------------------------
# Module 2: Cross-Sport Non-Overlapping Slips Generator
# -------------------------------------------------------------------
def build_universal_slip_subset(evaluated_candidates: List[Dict[str, Any]], consumed_fixtures: set, max_legs: int = 6) -> Tuple[List[Dict[str, Any]], float]:
    selected_legs = []
    sport_counts = {}
    current_odds = 1.0

    for leg in evaluated_candidates:
        if leg["fixture_id"] in consumed_fixtures:
            continue

        sport_name = leg["league_name"]
        # Limit matches per sport category to force genuine multi-sport mixing
        if sport_counts.get(sport_name, 0) >= 2:
            continue

        if len(selected_legs) >= max_legs:
            break

        selected_legs.append(leg)
        sport_counts[sport_name] = sport_counts.get(sport_name, 0) + 1
        consumed_fixtures.add(leg["fixture_id"])
        current_odds *= leg["odds"]

    return selected_legs, round(current_odds, 2)

async def generate_universal_expert_slips() -> List[str]:
    cached_matches = await load_future_fixtures()
    if not cached_matches:
        return ["⚠️ *Database empty.* Tap **📥 Fetch & Sync All Sports** first."]

    evaluated = []
    for f in cached_matches:
        cand = evaluate_market_across_sports(f)
        if cand:
            evaluated.append(cand)

    # Sort descending by statistical confidence probability
    evaluated.sort(key=lambda x: x["prob"], reverse=True)

    consumed_fixtures = set()

    # Slip 1 (Universal Expert Value Slip)
    slip_1_legs, slip_1_odds = build_universal_slip_subset(evaluated, consumed_fixtures, max_legs=6)

    # Slip 2 (Universal Expert High-Yield Slip - Zero Overlap)
    slip_2_legs, slip_2_odds = build_universal_slip_subset(evaluated, consumed_fixtures, max_legs=6)

    messages = []
    today_str = datetime.now(timezone.utc).strftime("%a, %d %b %Y")

    # Format Slip 1
    if len(slip_1_legs) >= 3:
        card_1 = [
            f"🎯 *CROSS-SPORT EXPERT SLIP 1*",
            f"📅 Date: `{today_str}`",
            f"📈 *Total Odds:* `{slip_1_odds:.2f}`",
            f"🔒 *Total Selections:* `{len(slip_1_legs)} Games`",
            "───────────────────────────\n"
        ]
        for idx, leg in enumerate(slip_1_legs, 1):
            card_1.append(
                f"*{idx}. {clean_md(leg['fixture'])}* (`{leg['kickoff']}`)\n"
                f"🏆 _{clean_md(leg['league_name'])}_\n"
                f"🎯 *{leg['pick']}* ({leg['category']}) @ `{leg['odds']:.2f}`\n"
                f"📊 Confidence: `{(leg['prob']*100):.1f}%`\n"
            )
        card_1.append("───────────────────────────")
        card_1.append("💡 *Blended selections across basketball, tennis, hockey, and soccer.*")
        messages.append("\n".join(card_1))
    else:
        messages.append("⚠️ *Slip 1:* Not enough high-probability matches across sports right now.")

    # Format Slip 2
    if len(slip_2_legs) >= 3:
        card_2 = [
            f"🔥 *CROSS-SPORT EXPERT SLIP 2*",
            f"📅 Date: `{today_str}`",
            f"📈 *Total Odds:* `{slip_2_odds:.2f}`",
            f"🔒 *Total Selections:* `{len(slip_2_legs)} Games`",
            "───────────────────────────\n"
        ]
        for idx, leg in enumerate(slip_2_legs, 1):
            card_2.append(
                f"*{idx}. {clean_md(leg['fixture'])}* (`{leg['kickoff']}`)\n"
                f"🏆 _{clean_md(leg['league_name'])}_\n"
                f"🎯 *{leg['pick']}* ({leg['category']}) @ `{leg['odds']:.2f}`\n"
                f"📊 Confidence: `{(leg['prob']*100):.1f}%`\n"
            )
        card_2.append("───────────────────────────")
        card_2.append("💡 *Zero fixture overlap with Slip 1 for maximum risk mitigation.*")
        messages.append("\n".join(card_2))
    else:
        messages.append("ℹ️ *Slip 2:* Need more distinct fixtures across sports for a second ticket.")

    return messages

# -------------------------------------------------------------------
# Telegram Keyboards & Router
# -------------------------------------------------------------------
def build_main_keyboard(cached_count: int) -> InlineKeyboardMarkup:
    keyboard = [
        [InlineKeyboardButton("📥 Fetch & Sync All Sports (72h)", callback_data="btn_read")],
        [InlineKeyboardButton("🎯 Generate 2 Cross-Sport Expert Slips", callback_data="gen_cross_sport")]
    ]
    return InlineKeyboardMarkup(keyboard)

async def start_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await init_db()
    count = await get_cached_fixtures_count()
    
    welcome_text = (
        "👑 *CROSS-SPORT PREDICTION HUB*\n"
        "───────────────────────────\n"
        f"📊 *Cached Fixtures:* `{count} Matches`\n\n"
        "Active engine scanning **Basketball, Tennis, Ice Hockey, Baseball, and Soccer** simultaneously. "
        "It cuts across sports categories to balance variance and outputs **2 non-overlapping expert slips**[span_0](start_span)[span_0](end_span).\n\n"
        "👇 *Select an action below:*"
    )
    
    await update.message.reply_text(
        welcome_text,
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
            text="🔄 *Syncing Basketball, Tennis, Hockey, Baseball & Soccer markets...*",
            parse_mode="Markdown"
        )
        res = await run_read_and_store_pipeline()
        await status.delete()

        count = await get_cached_fixtures_count()
        if res.get("success"):
            text = (
                "✅ *Universal Sync Complete!*\n"
                "───────────────────────────\n"
                f"• **Total Fixtures:** `{res['count']}`\n"
                f"• **Scanned Categories:** `{res['sports']} Sports/Leagues`\n\n"
                "👇 *Tap below to generate your cross-sport expert slips:*"
            )
        else:
            text = f"⚠️ *Sync Notice:* {res.get('message')}"

        await context.bot.send_message(
            chat_id=chat_id,
            text=text,
            parse_mode="Markdown",
            reply_markup=build_main_keyboard(count)
        )

    elif data == "gen_cross_sport":
        count = await get_cached_fixtures_count()
        if count == 0:
            await context.bot.send_message(
                chat_id=chat_id,
                text="⚠️ *Cache is empty.* Please tap **📥 Fetch & Sync All Sports (72h)** first.",
                parse_mode="Markdown",
                reply_markup=build_main_keyboard(0)
            )
            return

        status = await context.bot.send_message(
            chat_id=chat_id,
            text="⚙️ *Compounding cross-sport probabilities and formatting 2 non-overlapping expert slips...*",
            parse_mode="Markdown"
        )
        reports = await generate_universal_expert_slips()
        await status.delete()

        for report_text in reports:
            await context.bot.send_message(
                chat_id=chat_id,
                text=report_text,
                parse_mode="Markdown"
            )

# -------------------------------------------------------------------
# Application Entry Point
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

    print("🚀 Cross-Sport Prediction Bot active...")
    app.run_polling(drop_pending_updates=True)

if __name__ == "__main__":
    main()
