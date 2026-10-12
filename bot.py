import os
import time
import math
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

# -------------------------------------------------------------------
# Configuration Setup
# -------------------------------------------------------------------
load_dotenv()

TELEGRAM_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
ODDS_API_KEY = os.getenv("ODDS_API_KEY")
BASE_URL = "https://api.the-odds-api.com/v4/sports"
DB_NAME = "todays_predictions.db"

logging.basicConfig(format="%(asctime)s - %(name)s - %(levelname)s - %(message)s", level=logging.INFO)

GLOBAL_MASSIVE_LEAGUES = {
    "soccer_epl": "Premier League (ENG)",
    "soccer_efl_champ": "Championship (ENG)",
    "soccer_england_league1": "League One (ENG)",
    "soccer_england_efl_cup": "EFL Cup (ENG)",
    "soccer_fa_cup": "FA Cup (ENG)",
    "soccer_spain_la_liga": "La Liga (ESP)",
    "soccer_spain_segunda_division": "Segunda División (ESP)",
    "soccer_germany_bundesliga": "Bundesliga (GER)",
    "soccer_germany_bundesliga2": "2. Bundesliga (GER)",
    "soccer_italy_serie_a": "Serie A (ITA)",
    "soccer_italy_serie_b": "Serie B (ITA)",
    "soccer_france_ligue_one": "Ligue 1 (FRA)",
    "soccer_france_ligue_two": "Ligue 2 (FRA)",
    "soccer_uefa_champs_league": "UEFA Champions League",
    "soccer_uefa_europa_league": "UEFA Europa League",
    "soccer_uefa_conference_league": "UEFA Conference League",
    "soccer_netherlands_eredivisie": "Eredivisie (NED)",
    "soccer_portugal_primeira_liga": "Primeira Liga (POR)",
    "soccer_belgium_first_div": "Pro League (BEL)",
    "soccer_turkey_super_league": "Süper Lig (TUR)",
    "soccer_scotland_premiership": "Premiership (SCO)",
    "soccer_brazil_campeonato": "Série A (BRA)",
    "soccer_argentina_primera_division": "Primera (ARG)",
    "soccer_mexico_ligamx": "Liga MX (MEX)",
    "soccer_usa_mls": "MLS (USA)",
    "soccer_japan_j_league": "J1 League (JPN)",
    "soccer_korea_kleague1": "K League 1 (KOR)"
}

def clean_md(text: str) -> str:
    if not text:
        return ""
    for char in ["_", "*", "`", "[", "]", "(", ")"]:
        text = text.replace(char, " ")
    return " ".join(text.split())

# -------------------------------------------------------------------
# Mathematical Simulation Engine
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

def poisson_prob(lmbda: float, k: int) -> float:
    return (math.exp(-lmbda) * (lmbda ** k)) / math.factorial(k)

# -------------------------------------------------------------------
# High-Value Multi-Market Engine (Tuned for 1.55 - 1.95 Odds)
# -------------------------------------------------------------------
def select_best_dynamic_market(f: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    prices = [f["home_odds"], f["draw_odds"], f["away_odds"]]
    probs = devig_power_method(prices)
    if len(probs) < 3:
        return None

    home_p, draw_p, away_p = probs[0], probs[1], probs[2]
    home_name = clean_md(f["home_team"])
    away_name = clean_md(f["away_team"])

    home_xg = max(0.8, 1.25 + (home_p - away_p) * 1.6)
    away_xg = max(0.6, 0.95 + (away_p - home_p) * 1.3)
    total_xg = home_xg + away_xg

    candidates = []

    # 1. Higher-Value Outright Favorites (Odds: 1.55 to 1.92)
    if home_p >= 0.48 and 1.55 <= f["home_odds"] <= 1.92:
        candidates.append({
            "pick": f"{home_name} to Win",
            "category": "🏆 1X2",
            "odds": f["home_odds"],
            "prob": home_p
        })
    elif away_p >= 0.48 and 1.55 <= f["away_odds"] <= 1.92:
        candidates.append({
            "pick": f"{away_name} to Win",
            "category": "🏆 1X2",
            "odds": f["away_odds"],
            "prob": away_p
        })

    # 2. Over 2.5 Total Goals (Target odds: 1.60 to 1.95)
    prob_over_2_5 = sum(
        poisson_prob(home_xg, h) * poisson_prob(away_xg, a)
        for h in range(6) for a in range(6) if h + a > 2.5
    )
    if prob_over_2_5 >= 0.50:
        est_odds = round(1.0 / (prob_over_2_5 * 1.05), 2)
        if 1.55 <= est_odds <= 1.95:
            candidates.append({
                "pick": "Over 2.5 Goals",
                "category": "⚽ Goals",
                "odds": est_odds,
                "prob": prob_over_2_5
            })

    # 3. Both Teams To Score - GG Yes (Target odds: 1.55 to 1.90)
    prob_btts = (1.0 - poisson_prob(home_xg, 0)) * (1.0 - poisson_prob(away_xg, 0))
    if prob_btts >= 0.52:
        est_odds = round(1.0 / (prob_btts * 1.05), 2)
        if 1.52 <= est_odds <= 1.92:
            candidates.append({
                "pick": "Both Teams To Score (GG)",
                "category": "⚽ Goals",
                "odds": est_odds,
                "prob": prob_btts
            })

    # 4. Corners Over 9.5
    if total_xg >= 2.7:
        est_odds = 1.68
        prob_c = 0.58
        candidates.append({
            "pick": "Corners Over 9.5",
            "category": "🚩 Corners",
            "odds": est_odds,
            "prob": prob_c
        })

    if not candidates:
        return None

    candidates.sort(key=lambda x: (x["prob"] * x["odds"]), reverse=True)
    best = candidates[0]
    best["fixture_id"] = f["fixture_id"]
    best["fixture"] = f"{home_name} vs {away_name}"
    best["league_name"] = f["league_name"]

    try:
        dt = datetime.fromisoformat(f["commence_time"].replace('Z', '+00:00'))
        best["kickoff"] = dt.strftime("%a %H:%M UTC")
    except Exception:
        best["kickoff"] = "Upcoming"

    return best

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
# Module 1: READ Pipeline
# -------------------------------------------------------------------
async def run_read_and_store_pipeline() -> Dict[str, Any]:
    if not ODDS_API_KEY:
        return {"success": False, "message": "ODDS_API_KEY missing in .env"}

    active_leagues = {}
    normalized_fixtures = []

    now_utc = datetime.now(timezone.utc)
    window_start = now_utc - timedelta(hours=1)
    window_end = now_utc + timedelta(hours=60)

    async with aiohttp.ClientSession() as session:
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

        combined_league_pool = {**GLOBAL_MASSIVE_LEAGUES, **active_leagues}
        sem = asyncio.Semaphore(7)

        async def fetch_league_matches(sport_key: str, label: str):
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

        tasks = [fetch_league_matches(k, v) for k, v in combined_league_pool.items()]
        batch_results = await asyncio.gather(*tasks)
        for res in batch_results:
            normalized_fixtures.extend(res)

    if normalized_fixtures:
        await store_fixtures_to_db(normalized_fixtures)
        return {
            "success": True, 
            "count": len(normalized_fixtures), 
            "leagues": len(combined_league_pool)
        }

    return {"success": False, "message": "Zero active fixtures found within rolling horizon."}

# -------------------------------------------------------------------
# Module 2: PREDICT (High-Yield Generator for 100/200 Odds with <= 10 Teams)
# -------------------------------------------------------------------
async def generate_high_yield_slip(target_odds: float, max_legs: int) -> str:
    cached_matches = await load_future_fixtures()
    if not cached_matches:
        return "⚠️ *Database empty.* Tap **📥 Fetch & Sync Matches** first[cite: 11]."

    evaluated = []
    for f in cached_matches:
        cand = select_best_dynamic_market(f)
        if cand:
            evaluated.append(cand)

    evaluated.sort(key=lambda x: x["prob"], reverse=True)

    selected_legs = []
    league_counts = {}
    current_odds = 1.0

    for leg in evaluated:
        l_name = leg["league_name"]
        if league_counts.get(l_name, 0) >= 1:
            continue  # Enforce 1 pick per league

        if len(selected_legs) >= max_legs:
            break

        selected_legs.append(leg)
        league_counts[l_name] = league_counts.get(l_name, 0) + 1
        current_odds *= leg["odds"]

        if current_odds >= target_odds:
            break

    if len(selected_legs) < 4:
        return (
            "⚠️ *Not enough high-yield fixtures available right now.*\n\n"
            "Tap **📥 Fetch & Sync Matches** later as bookmakers post new lines."
        )

    target_label = int(target_odds)
    report = [
        f"🎯 *HIGH-YIELD {target_label} ODDS ACCUMULATOR*",
        f"📊 *Strategy:* `Max {max_legs} Teams (High-Value Selections)`",
        f"📈 *Total Combined Odds:* `{current_odds:.2f}`",
        f"🔒 *Total Matches:* `{len(selected_legs)} Games`",
        "───────────────────────────\n"
    ]

    for idx, leg in enumerate(selected_legs, 1):
        report.append(
            f"*{idx}. {clean_md(leg['fixture'])}* (`{leg['kickoff']}`)\n"
            f"🏆 _{clean_md(leg['league_name'])}_\n"
            f"🎯 *{leg['pick']}* ({leg['category']}) @ `{leg['odds']:.2f}`\n"
            f"📊 Confidence: `{(leg['prob']*100):.1f}%`\n"
        )

    report.append("───────────────────────────")
    report.append("💡 *Optimized for maximum payout with compact leg limits.*")
    return "\n".join(report)

# -------------------------------------------------------------------
# Redesigned UI Keyboards & Router
# -------------------------------------------------------------------
def build_main_keyboard(cached_count: int) -> InlineKeyboardMarkup:
    keyboard = [
        [InlineKeyboardButton("📥 Fetch & Sync Matches (48h)", callback_data="btn_read")],
        [
            InlineKeyboardButton("🔥 Generate 100 Odds (Max 9 Teams)", callback_data="pred_100")
        ],
        [
            InlineKeyboardButton("🚀 Generate 200 Odds (Max 10 Teams)", callback_data="pred_200")
        ]
    ]
    return InlineKeyboardMarkup(keyboard)

async def start_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await init_db()
    count = await get_cached_fixtures_count()
    
    welcome_text = (
        "👑 *HIGH-YIELD ACCUMULATOR HUB*\n"
        "───────────────────────────\n"
        f"📊 *Database Status:* `{count} Matches Cached`\n\n"
        "Welcome to your professional football analytics dashboard. "
        "Our engine constructs high-payout accumulators using **10 or fewer teams**, "
        "leveraging high-value selections (1.55 – 1.95 odds) to reduce parlay risks.\n\n"
        "👇 *Select an action below to get started:*"
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
            text="🔄 *Syncing global leagues... Please wait.*",
            parse_mode="Markdown"
        )
        res = await run_read_and_store_pipeline()
        await status.delete()

        count = await get_cached_fixtures_count()
        if res.get("success"):
            text = (
                "✅ *Synchronization Successful!*\n"
                "───────────────────────────\n"
                f"• **Indexed Fixtures:** `{res['count']}`\n"
                f"• **Active Competitions:** `{res['leagues']}`\n\n"
                "👇 *Choose your target high-yield slip below:*"
            )
        else:
            text = f"⚠️ *Sync Notice:* {res.get('message')}"

        await context.bot.send_message(
            chat_id=chat_id,
            text=text,
            parse_mode="Markdown",
            reply_markup=build_main_keyboard(count)
        )

    elif data in ["pred_100", "pred_200"]:
        count = await get_cached_fixtures_count()
        if count == 0:
            await context.bot.send_message(
                chat_id=chat_id,
                text="⚠️ *Cache is empty.* Please tap **📥 Fetch & Sync Matches (48h)** first[cite: 11].",
                parse_mode="Markdown",
                reply_markup=build_main_keyboard(0)
            )
            return

        target = 100.0 if data == "pred_100" else 200.0
        max_legs = 9 if data == "pred_100" else 10

        status = await context.bot.send_message(
            chat_id=chat_id,
            text=f"⚙️ *Compounding high-value markets for ~{int(target)} odds slip (Max {max_legs} teams)...*",
            parse_mode="Markdown"
        )
        report = await generate_high_yield_slip(target_odds=target, max_legs=max_legs)
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

    print("🚀 High-Yield Bot active with redesigned UI...")
    app.run_polling(drop_pending_updates=True)

if __name__ == "__main__":
    main()