import os
import time
import math
import logging
import aiohttp
import asyncio
import aiosqlite
from typing import List, Dict, Any
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
# Environment & Configuration Setup (.env Integration)
# -------------------------------------------------------------------
load_dotenv()

TELEGRAM_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
ODDS_API_KEY = os.getenv("ODDS_API_KEY")
BASE_URL = "https://api.the-odds-api.com/v4/sports"
DB_NAME = "todays_predictions.db"

logging.basicConfig(format="%(asctime)s - %(name)s - %(levelname)s - %(message)s", level=logging.INFO)

ODDS_CACHE: Dict[str, Dict[str, Any]] = {}
CACHE_TTL = 1800  # 30 Minutes cache TTL
CACHE_LOCK = asyncio.Lock()

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
    "soccer_argentina_primera_division": "Primera División - Argentina",
    "soccer_mexico_ligamx": "Liga MX",
    "soccer_usa_mls": "MLS",
    "soccer_china_super_league": "Super League - China",
    "soccer_japan_j_league": "J-League",
    "soccer_chile_camp_nacional": "Chile Primera",
    "soccer_sweden_allsvenskan": "Allsvenskan",
    "soccer_norway_eliteserien": "Eliteserien"
}

def clean_md(text: str) -> str:
    if not text:
        return ""
    for char in ["_", "*", "`", "[", "]", "(", ")"]:
        text = text.replace(char, " ")
    return " ".join(text.split())

# -------------------------------------------------------------------
# Simulation Engine
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

def generate_multi_market_projections(home_p: float, draw_p: float, away_p: float) -> Dict[str, Any]:
    home_xg = max(0.8, 1.25 + (home_p - away_p) * 1.6)
    away_xg = max(0.6, 0.95 + (away_p - home_p) * 1.3)
    total_xg = home_xg + away_xg

    prob_over_1_5 = sum(poisson_prob(home_xg, h) * poisson_prob(away_xg, a) 
                        for h in range(6) for a in range(6) if h + a > 1.5)
    prob_over_2_5 = sum(poisson_prob(home_xg, h) * poisson_prob(away_xg, a) 
                        for h in range(6) for a in range(6) if h + a > 2.5)
    
    p_home_0 = poisson_prob(home_xg, 0)
    p_away_0 = poisson_prob(away_xg, 0)
    prob_btts_yes = (1.0 - p_home_0) * (1.0 - p_away_0)

    est_corners = round(8.5 + (total_xg * 0.95), 1)
    prob_over_8_5_corners = min(0.88, max(0.42, 0.50 + (est_corners - 9.5) * 0.12))

    parity_factor = 1.0 - abs(home_p - away_p)
    est_cards = round(3.2 + (parity_factor * 1.6), 1)
    prob_over_3_5_cards = min(0.85, max(0.38, 0.48 + (est_cards - 4.0) * 0.14))

    dc_1x = (home_p + draw_p) * 100
    dc_x2 = (away_p + draw_p) * 100

    return {
        "home_xg": round(home_xg, 2),
        "away_xg": round(away_xg, 2),
        "total_xg": round(total_xg, 2),
        "over_1_5_pct": round(prob_over_1_5 * 100, 1),
        "over_2_5_pct": round(prob_over_2_5 * 100, 1),
        "btts_pct": round(prob_btts_yes * 100, 1),
        "est_corners": est_corners,
        "over_8_5_corners_pct": round(prob_over_8_5_corners * 100, 1),
        "est_cards": est_cards,
        "over_3_5_cards_pct": round(prob_over_3_5_cards * 100, 1),
        "dc_1x_pct": round(dc_1x, 1),
        "dc_x2_pct": round(dc_x2, 1)
    }

# -------------------------------------------------------------------
# Database Engine (Guaranteed Initialization)
# -------------------------------------------------------------------
async def init_db():
    async with aiosqlite.connect(DB_NAME) as db:
        await db.execute("""
            CREATE TABLE IF NOT EXISTS predictions_history (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                match_name TEXT,
                league TEXT,
                main_pick TEXT,
                goal_pick TEXT,
                corner_card_pick TEXT,
                match_date TEXT,
                timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
            )
        """)
        await db.commit()
    logging.info("✅ Database schema initialized successfully!")

async def save_predictions_batch(matches_data: List[Dict[str, Any]]):
    await init_db()  # Safety check
    today_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    async with aiosqlite.connect(DB_NAME) as db:
        for m in matches_data:
            async with db.execute("""
                SELECT id FROM predictions_history 
                WHERE match_name = ? AND match_date = ?
            """, (m["match_name"], today_str)) as cursor:
                if not await cursor.fetchone():
                    await db.execute("""
                        INSERT INTO predictions_history 
                        (match_name, league, main_pick, goal_pick, corner_card_pick, match_date)
                        VALUES (?, ?, ?, ?, ?, ?)
                    """, (m["match_name"], m["league"], m["main_pick"], m["goal_pick"], m["corner_card_pick"], today_str))
        await db.commit()

async def get_history_logs(limit: int = 20) -> List[Dict[str, Any]]:
    await init_db()  # Safety check
    async with aiosqlite.connect(DB_NAME) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute("""
            SELECT match_name, league, main_pick, goal_pick, corner_card_pick, match_date
            FROM predictions_history
            ORDER BY id DESC
            LIMIT ?
        """, (limit,)) as cursor:
            rows = await cursor.fetchall()
            return [dict(row) for row in rows]

# -------------------------------------------------------------------
# Odds Ingestion Engine
# -------------------------------------------------------------------
async def fetch_todays_matches() -> List[Dict[str, Any]]:
    now_time = time.time()
    cache_key = "todays_fixtures_pure_live"

    if cache_key in ODDS_CACHE and (now_time - ODDS_CACHE[cache_key]["timestamp"] < CACHE_TTL):
        return ODDS_CACHE[cache_key]["data"]

    async with CACHE_LOCK:
        if cache_key in ODDS_CACHE and (now_time - ODDS_CACHE[cache_key]["timestamp"] < CACHE_TTL):
            return ODDS_CACHE[cache_key]["data"]

        todays_matches = []

        if ODDS_API_KEY and len(ODDS_API_KEY) > 10:
            try:
                now_utc = datetime.now(timezone.utc)
                window_start = now_utc - timedelta(hours=3)
                window_end = now_utc + timedelta(hours=36)

                async with aiohttp.ClientSession() as session:
                    url = f"{BASE_URL}?apiKey={ODDS_API_KEY}"
                    async with session.get(url, timeout=8) as resp:
                        if resp.status == 200:
                            data = await resp.json()
                            active_leagues = {
                                item["key"]: item.get("title", item["key"]) 
                                for item in data 
                                if item.get("key", "").startswith("soccer_") and item.get("active", False)
                            }
                            if not active_leagues:
                                active_leagues = GLOBAL_SOCCER_LEAGUES

                            sem = asyncio.Semaphore(5)
                            async def fetch_league_odds(sport_key: str, label: str):
                                async with sem:
                                    odds_url = f"{BASE_URL}/{sport_key}/odds/"
                                    params = {
                                        "apiKey": ODDS_API_KEY, 
                                        "regions": "uk,eu,us", 
                                        "markets": "h2h", 
                                        "oddsFormat": "decimal"
                                    }
                                    try:
                                        async with session.get(odds_url, params=params, timeout=8) as o_resp:
                                            if o_resp.status == 200:
                                                fixtures = await o_resp.json()
                                                matched = []
                                                for fixture in fixtures:
                                                    commence_raw = fixture.get("commence_time", "")
                                                    if commence_raw:
                                                        commence_dt = datetime.fromisoformat(commence_raw.replace('Z', '+00:00'))
                                                        if window_start <= commence_dt <= window_end:
                                                            fixture["league_name"] = label
                                                            matched.append(fixture)
                                                return matched
                                    except Exception:
                                        pass
                                    return []

                            tasks = [fetch_league_odds(key, label) for key, label in active_leagues.items()]
                            results = await asyncio.gather(*tasks)
                            for res in results:
                                todays_matches.extend(res)
                        else:
                            logging.error(f"Odds API Error [{resp.status}]: Monthly quota exhausted or invalid key.")
            except Exception as e:
                logging.error(f"Live Odds Fetch Exception: {e}")

        ODDS_CACHE[cache_key] = {"timestamp": now_time, "data": todays_matches}
        return todays_matches

# -------------------------------------------------------------------
# Telegram Keyboards & Safe Chunk Sender
# -------------------------------------------------------------------
def build_main_menu() -> InlineKeyboardMarkup:
    keyboard = [
        [
            InlineKeyboardButton("🎯 Generate Predictions (Top 20 Matches)", callback_data="run_today_all")
        ],
        [
            InlineKeyboardButton("📜 Prediction History Log", callback_data="run_history"),
            InlineKeyboardButton("💡 Staking Advice", callback_data="run_advice")
        ]
    ]
    return InlineKeyboardMarkup(keyboard)

async def start_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await init_db()
    today_formatted = datetime.now(timezone.utc).strftime("%B %d, %Y")
    await update.message.reply_text(
        f"🚀 *Sports Analytics Hub* ({today_formatted})\n\n"
        "Tap a button below to generate multi-market match predictions, review prediction logs, or view staking rules:",
        parse_mode="Markdown",
        reply_markup=build_main_menu()
    )

async def send_clean_chunks(bot, chat_id: int, header: str, cards: List[str]):
    current_chunk = header
    for card in cards:
        if len(current_chunk) + len(card) > 3500:
            await bot.send_message(chat_id=chat_id, text=current_chunk, parse_mode="Markdown")
            current_chunk = card
        else:
            current_chunk += card

    if current_chunk:
        await bot.send_message(chat_id=chat_id, text=current_chunk, parse_mode="Markdown")

async def button_router(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    data = query.data
    chat_id = query.message.chat_id

    try:
        if data == "run_today_all":
            status_msg = await context.bot.send_message(chat_id=chat_id, text="⏳ *Scanning live global leagues for upcoming predictions...*", parse_mode="Markdown")
            header, cards = await generate_todays_full_report()
            await status_msg.delete()
            
            if not cards:
                await context.bot.send_message(chat_id=chat_id, text=header, parse_mode="Markdown")
            else:
                await send_clean_chunks(context.bot, chat_id, header, cards)

        elif data == "run_history":
            status_msg = await context.bot.send_message(chat_id=chat_id, text="📜 *Retrieving logged prediction history...*", parse_mode="Markdown")
            header, cards = await generate_history_report()
            await status_msg.delete()
            
            if not cards:
                await context.bot.send_message(chat_id=chat_id, text=header, parse_mode="Markdown")
            else:
                await send_clean_chunks(context.bot, chat_id, header, cards)

        elif data == "run_advice":
            advice_text = get_staking_advice_text()
            await context.bot.send_message(chat_id=chat_id, text=advice_text, parse_mode="Markdown")

    except Exception as e:
        logging.error(f"Error executing callback {data}: {e}")
        await context.bot.send_message(
            chat_id=chat_id,
            text=f"⚠️ *An error occurred:* `{clean_md(str(e))}`",
            parse_mode="Markdown"
        )

# -------------------------------------------------------------------
# Clean Report Generators
# -------------------------------------------------------------------
async def generate_todays_full_report() -> tuple:
    matches = await fetch_todays_matches()
    today_str = datetime.now(timezone.utc).strftime("%d %b %Y")

    if not matches:
        return (
            f"ℹ️ *No active live matches found in the next 36 hours.* \n\n"
            "This occurs when your monthly API quota is exhausted or when bookmakers haven't posted head-to-head lines yet. "
            "You can still check past predictions under **Prediction History Log**.", 
            []
        )

    target_matches = matches[:20]
    saved_batch = []
    cards = []

    header = f"📅 *LIVE MATCH PREDICTIONS* (`{today_str}`)\n"
    header += f"📊 Fixtures: `{len(target_matches)}` | Picks: `{len(target_matches) * 3}`\n"
    header += "───────────────────────────\n\n"

    for idx, fixture in enumerate(target_matches, 1):
        home = clean_md(fixture.get("home_team", "Home"))
        away = clean_md(fixture.get("away_team", "Away"))
        league = clean_md(fixture.get("league_name", "League").replace("⚽ ", ""))
        bookies = fixture.get("bookmakers", [])

        if not bookies:
            continue

        h2h = next((m for b in bookies for m in b.get("markets", []) if m["key"] == "h2h"), None)
        if not h2h:
            continue

        outcomes = h2h.get("outcomes", [])
        if len(outcomes) < 2:
            continue

        prices = [o["price"] for o in outcomes]
        fair_probs = devig_power_method(prices)
        if len(fair_probs) < 2:
            continue

        home_p = fair_probs[0]
        draw_p = fair_probs[1] if len(fair_probs) == 3 else 0.25
        away_p = fair_probs[-1]

        p = generate_multi_market_projections(home_p, draw_p, away_p)

        if home_p >= 0.50:
            main_pick = f"Home Win ({home_p*100:.1f}%)"
        elif away_p >= 0.50:
            main_pick = f"Away Win ({away_p*100:.1f}%)"
        else:
            main_pick = f"1X Double Chance ({p['dc_1x_pct']}%)"

        goal_pick = f"Over 2.5 ({p['over_2_5_pct']}%)" if p['over_2_5_pct'] > 50 else f"Over 1.5 ({p['over_1_5_pct']}%)"
        corner_card_pick = f"Corners >8.5 | Cards >3.5"

        match_name = f"{home} vs {away}"
        saved_batch.append({
            "match_name": match_name,
            "league": league,
            "main_pick": main_pick,
            "goal_pick": goal_pick,
            "corner_card_pick": corner_card_pick
        })

        card_str = (
            f"*{idx}. {match_name}*\n"
            f"🏆 _{league}_\n"
            f"🎯 *Pick:* `{main_pick}`\n"
            f"⚽ *Goals:* `{goal_pick}` | `BTTS: {p['btts_pct']}%`\n"
            f"📊 *Stats:* `{corner_card_pick}`\n"
            f"───────────────────────────\n\n"
        )
        cards.append(card_str)

    if saved_batch:
        await save_predictions_batch(saved_batch)

    return header, cards

async def generate_history_report() -> tuple:
    logs = await get_history_logs(limit=20)
    if not logs:
        return "📜 *Prediction History Log*\n\nNo prediction records found in the database.", []

    header = "📜 *PREDICTION HISTORY LOG*\n───────────────────────────\n\n"
    cards = []

    for idx, log in enumerate(logs, 1):
        match_name = clean_md(log['match_name'])
        league = clean_md(log['league'])
        main_pick = clean_md(log['main_pick'])
        goal_pick = clean_md(log['goal_pick'])
        corner_card_pick = clean_md(log['corner_card_pick'])

        card_str = (
            f"*{idx}. {match_name}* (`{log['match_date']}`)\n"
            f"🏆 _{league}_\n"
            f"🎯 *Pick:* `{main_pick}`\n"
            f"⚽ *Goals:* `{log['goal_pick']}`\n"
            f"📊 *Stats:* `{corner_card_pick}`\n"
            f"───────────────────────────\n\n"
        )
        cards.append(card_str)

    return header, cards

def get_staking_advice_text() -> str:
    return (
        "💡 *PROFESSIONAL STAKING GUIDELINES*\n"
        "───────────────────────────\n\n"
        "1️⃣ *The 1% - 3% Unit System*\n"
        "• Define your bankroll (e.g., $100 or $1,000).\n"
        "• **1 Unit = 1% to 2%** of your total bankroll. Never stake more than 3% on a single bet.\n\n"
        "2️⃣ *Fractional Kelly Sizing*\n"
        "• Scale your bet size dynamically based on confidence. High probability = **2 Units**, Moderate probability = **1 Unit**.\n\n"
        "3️⃣ *Avoid Heavy Parlays*\n"
        "• Multi-leg parlays compound bookmaker margins.\n"
        "• Limit accumulators to **2–3 high-probability legs** (e.g., Double Chance or Over 1.5 Goals).\n\n"
        "4️⃣ *Disciplined Record Keeping*\n"
        "• Track every stake and measure performance by monthly yield."
    )

# -------------------------------------------------------------------
# Entry Point
# -------------------------------------------------------------------
async def startup_initialization():
    await init_db()

def main():
    if not TELEGRAM_TOKEN:
        raise ValueError("TELEGRAM_BOT_TOKEN is missing or not set in .env!")

    # Force DB init on startup loop
    asyncio.run(startup_initialization())

    app = ApplicationBuilder().token(TELEGRAM_TOKEN).build()
    app.add_handler(CommandHandler("start", start_command))
    app.add_handler(CallbackQueryHandler(button_router))

    print("🚀 Bot initialized! Table predictions_history created successfully.")
    app.run_polling()

if __name__ == "__main__":
    main()
