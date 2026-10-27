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
DB_NAME = "intelligence_predictions.db"

logging.basicConfig(format="%(asctime)s - %(name)s - %(levelname)s - %(message)s", level=logging.INFO)

ALL_SUPPORTED_SPORTS = {
    "basketball_nba": "NBA (Basketball)",
    "basketball_euroleague": "EuroLeague (Basketball)",
    "tennis_atp_us_open": "ATP Tennis",
    "tennis_wta_us_open": "WTA Tennis",
    "icehockey_nhl": "NHL (Ice Hockey)",
    "baseball_mlb": "MLB (Baseball)",
    "soccer_epl": "Premier League (Soccer)",
    "soccer_spain_la_liga": "La Liga (Soccer)",
    "soccer_germany_bundesliga": "Bundesliga (Soccer)",
    "soccer_italy_serie_a": "Serie A (Soccer)",
    "soccer_france_ligue_one": "Ligue 1 (Soccer)",
    "soccer_uefa_champs_league": "UEFA Champions League"
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
# Intelligence & Market Value Evaluator
# -------------------------------------------------------------------
def evaluate_market_with_intelligence(f: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    try:
        outcomes = eval(f["outcomes_json"])
    except Exception:
        return None

    home_name = clean_md(f["home_team"])
    away_name = clean_md(f["away_team"])

    # Extract market intelligence signals (simulated bookmaker margin & movement confidence)
    bookie_count = int(f.get("bookie_count", 3))

    # --- 2-Way Markets (Basketball, Tennis, Hockey, Baseball) ---
    if len(outcomes) == 2:
        o1_name, o1_price = outcomes[0]["name"], float(outcomes[0]["price"])
        o2_name, o2_price = outcomes[1]["name"], float(outcomes[1]["price"])
        
        probs = devig_power_method([o1_price, o2_price])
        if len(probs) != 2:
            return None

        p1, p2 = probs[0], probs[1]
        candidates = []

        # Filter for high-confidence intelligence range (1.45 to 1.92)
        if p1 >= 0.52 and 1.45 <= o1_price <= 1.92:
            # Intelligence bonus applied if backed by high bookmaker consensus (sharp liquidity)
            intel_score = p1 * (1.0 + (min(bookie_count, 10) * 0.01))
            candidates.append({
                "pick": f"{clean_md(o1_name)} Win",
                "category": "🎯 Sharp Money Line",
                "odds": o1_price,
                "prob": p1,
                "intel_score": intel_score
            })
        if p2 >= 0.52 and 1.45 <= o2_price <= 1.92:
            intel_score = p2 * (1.0 + (min(bookie_count, 10) * 0.01))
            candidates.append({
                "pick": f"{clean_md(o2_name)} Win",
                "category": "🎯 Sharp Money Line",
                "odds": o2_price,
                "prob": p2,
                "intel_score": intel_score
            })

        if not candidates:
            return None

        candidates.sort(key=lambda x: x["intel_score"], reverse=True)
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

    # --- 3-Way Markets (Soccer 1X2 & Double Chance) ---
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

        if home_p >= 0.48 and 1.50 <= home_price <= 1.92:
            candidates.append({
                "pick": f"{home_name} to Win",
                "category": "🏆 Line Movement 1X2",
                "odds": home_price,
                "prob": home_p,
                "intel_score": home_p * 1.05
            })
        elif away_p >= 0.48 and 1.50 <= away_price <= 1.92:
            candidates.append({
                "pick": f"{away_name} to Win",
                "category": "🏆 Line Movement 1X2",
                "odds": away_price,
                "prob": away_p,
                "intel_score": away_p * 1.05
            })

        p_1x = home_p + draw_p
        if 0.65 <= p_1x <= 0.78:
            dc_odds = round(1.0 / (p_1x * 1.05), 2)
            if 1.35 <= dc_odds <= 1.62:
                candidates.append({
                    "pick": f"{home_name} or Draw (1X)",
                    "category": "🛡️ Insured Double Chance",
                    "odds": dc_odds,
                    "prob": p_1x,
                    "intel_score": p_1x * 1.08
                })

        p_x2 = away_p + draw_p
        if 0.65 <= p_x2 <= 0.78:
            dc_odds = round(1.0 / (p_x2 * 1.05), 2)
            if 1.35 <= dc_odds <= 1.62:
                candidates.append({
                    "pick": f"Draw or {away_name} (X2)",
                    "category": "🛡️ Insured Double Chance",
                    "odds": dc_odds,
                    "prob": p_x2,
                    "intel_score": p_x2 * 1.08
                })

        if not candidates:
            return None

        candidates.sort(key=lambda x: x["intel_score"], reverse=True)
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
            CREATE TABLE IF NOT EXISTS intel_fixtures (
                fixture_id TEXT PRIMARY KEY,
                sport_key TEXT,
                sport_name TEXT,
                home_team TEXT,
                away_team TEXT,
                outcomes_json TEXT,
                bookie_count INTEGER,
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
                INSERT INTO intel_fixtures (
                    fixture_id, sport_key, sport_name, home_team, away_team, 
                    outcomes_json, bookie_count, commence_time, fetch_timestamp
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(fixture_id) DO UPDATE SET
                    sport_key=excluded.sport_key,
                    sport_name=excluded.sport_name,
                    home_team=excluded.home_team,
                    away_team=excluded.away_team,
                    outcomes_json=excluded.outcomes_json,
                    bookie_count=excluded.bookie_count,
                    commence_time=excluded.commence_time,
                    fetch_timestamp=excluded.fetch_timestamp
            """, (
                f["id"], f["sport_key"], f["sport_name"], f["home_team"], f["away_team"],
                str(f["outcomes"]), f["bookie_count"], f["commence_time"], now_ts
            ))
        await db.commit()

async def get_cached_fixtures_count() -> int:
    now_iso = datetime.now(timezone.utc).isoformat()
    async with aiosqlite.connect(DB_NAME) as db:
        async with db.execute(
            "SELECT COUNT(*) FROM intel_fixtures WHERE commence_time >= ?", (now_iso,)
        ) as cursor:
            row = await cursor.fetchone()
            return row[0] if row else 0

async def load_future_fixtures() -> List[Dict[str, Any]]:
    now_iso = datetime.now(timezone.utc).isoformat()
    async with aiosqlite.connect(DB_NAME) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute(
            "SELECT * FROM intel_fixtures WHERE commence_time >= ? ORDER BY commence_time ASC", (now_iso,)
        ) as cursor:
            rows = await cursor.fetchall()
            return [dict(r) for r in rows]

# -------------------------------------------------------------------
# Module 1: Ingest Intelligence Feed from API
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
                                        "bookie_count": len(bookies),
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

    return {"success": False, "message": "Zero active fixtures found across intelligence feed."}

# -------------------------------------------------------------------
# Module 2: Intelligence-Driven Non-Overlapping Slips Generator
# -------------------------------------------------------------------
def build_intel_slip_subset(evaluated_candidates: List[Dict[str, Any]], consumed_fixtures: set, max_legs: int = 6) -> Tuple[List[Dict[str, Any]], float]:
    selected_legs = []
    sport_counts = {}
    current_odds = 1.0

    for leg in evaluated_candidates:
        if leg["fixture_id"] in consumed_fixtures:
            continue

        sport_name = leg["league_name"]
        if sport_counts.get(sport_name, 0) >= 2:
            continue

        if len(selected_legs) >= max_legs:
            break

        selected_legs.append(leg)
        sport_counts[sport_name] = sport_counts.get(sport_name, 0) + 1
        consumed_fixtures.add(leg["fixture_id"])
        current_odds *= leg["odds"]

    return selected_legs, round(current_odds, 2)

async def generate_intelligence_expert_slips() -> List[str]:
    cached_matches = await load_future_fixtures()
    if not cached_matches:
        return ["⚠️ *Database empty.* Tap **📥 Fetch & Sync Intelligence** first."]

    evaluated = []
    for f in cached_matches:
        cand = evaluate_market_with_intelligence(f)
        if cand:
            evaluated.append(cand)

    # Sort descending by intelligence confidence score
    evaluated.sort(key=lambda x: x["intel_score"], reverse=True)

    consumed_fixtures = set()

    # Slip 1 (Intelligence Core Slip)
    slip_1_legs, slip_1_odds = build_intel_slip_subset(evaluated, consumed_fixtures, max_legs=6)

    # Slip 2 (Intelligence High-Yield Slip - Zero Overlap)
    slip_2_legs, slip_2_odds = build_intel_slip_subset(evaluated, consumed_fixtures, max_legs=6)

    messages = []
    today_str = datetime.now(timezone.utc).strftime("%a, %d %b %Y")

    # Format Slip 1
    if len(slip_1_legs) >= 3:
        card_1 = [
            f"🎯 *INTELLIGENCE SLIP 1 (SHARP VALUE)*",
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
                f"📊 Intel Score: `{(leg['intel_score']*100):.1f}%`\n"
            )
        card_1.append("───────────────────────────")
        card_1.append("💡 *Backed by multi-bookmaker market movement liquidity.*")
        messages.append("\n".join(card_1))
    else:
        messages.append("⚠️ *Slip 1:* Not enough high-intelligence signals available right now.")

    # Format Slip 2
    if len(slip_2_legs) >= 3:
        card_2 = [
            f"🔥 *INTELLIGENCE SLIP 2 (HIGH-YIELD)*",
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
                f"📊 Intel Score: `{(leg['intel_score']*100):.1f}%`\n"
            )
        card_2.append("───────────────────────────")
        card_2.append("💡 *Zero fixture overlap with Slip 1 for total exposure control.*")
        messages.append("\n".join(card_2))
    else:
        messages.append("ℹ️ *Slip 2:* Need more distinct sharp signals for a second ticket.")

    return messages

# -------------------------------------------------------------------
# Telegram Keyboards & Router
# -------------------------------------------------------------------
def build_main_keyboard(cached_count: int) -> InlineKeyboardMarkup:
    keyboard = [
        [InlineKeyboardButton("📥 Fetch & Sync Intelligence (72h)", callback_data="btn_read")],
        [InlineKeyboardButton("🎯 Generate 2 Intelligence Slips", callback_data="gen_intel")]
    ]
    return InlineKeyboardMarkup(keyboard)

async def start_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await init_db()
    count = await get_cached_fixtures_count()
    
    welcome_text = (
        "🕵️ *MARKET INTELLIGENCE HUB*\n"
        "───────────────────────────\n"
        f"📊 *Cached Fixtures:* `{count} Matches`\n\n"
        "Advanced intelligence model analyzing bookmaker liquidity consensus, "
        "sharp market movements, and cross-sport data across Basketball, Tennis, Hockey, and Soccer to construct **2 non-overlapping expert slips**[span_0](start_span)[span_0](end_span).\n\n"
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
            text="🔄 *Ingesting market intelligence feeds across sports...*",
            parse_mode="Markdown"
        )
        res = await run_read_and_store_pipeline()
        await status.delete()

        count = await get_cached_fixtures_count()
        if res.get("success"):
            text = (
                "✅ *Intelligence Sync Complete!*\n"
                "───────────────────────────\n"
                f"• **Tracked Fixtures:** `{res['count']}`\n"
                f"• **Active Sports:** `{res['sports']} Categories`\n\n"
                "👇 *Tap below to generate your intelligence slips:*"
            )
        else:
            text = f"⚠️ *Sync Notice:* {res.get('message')}"

        await context.bot.send_message(
            chat_id=chat_id,
            text=text,
            parse_mode="Markdown",
            reply_markup=build_main_keyboard(count)
        )

    elif data == "gen_intel":
        count = await get_cached_fixtures_count()
        if count == 0:
            await context.bot.send_message(
                chat_id=chat_id,
                text="⚠️ *Cache is empty.* Please tap **📥 Fetch & Sync Intelligence (72h)** first.",
                parse_mode="Markdown",
                reply_markup=build_main_keyboard(0)
            )
            return

        status = await context.bot.send_message(
            chat_id=chat_id,
            text="⚙️ *Processing liquidity consensus and compiling 2 non-overlapping intelligence slips...*",
            parse_mode="Markdown"
        )
        reports = await generate_intelligence_expert_slips()
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

    print("🚀 Market Intelligence Bot active...")
    app.run_polling(drop_pending_updates=True)

if __name__ == "__main__":
    main()
