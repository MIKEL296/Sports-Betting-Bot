import os
import time
import sqlite3
import logging
import aiohttp
from dotenv import load_dotenv
from typing import List, Dict, Any

from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    ApplicationBuilder,
    CommandHandler,
    CallbackQueryHandler,
    ContextTypes
)

from math_engine import (
    devig_power_method,
    calculate_ev,
    calculate_kelly_stake,
    scan_match_arbitrage
)

# -------------------------------------------------------------------
# Configuration & Environment Setup
# -------------------------------------------------------------------
load_dotenv()
TELEGRAM_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
ODDS_API_KEY = os.getenv("ODDS_API_KEY")
BASE_URL = "https://api.the-odds-api.com/v4/sports"
DB_NAME = "predictions.db"

logging.basicConfig(format="%(asctime)s - %(name)s - %(levelname)s - %(message)s", level=logging.INFO)

# User Session Settings Store
USER_SETTINGS: Dict[int, Dict[str, Any]] = {}

# -------------------------------------------------------------------
# SQLite Database Storage Setup
# -------------------------------------------------------------------
def init_db():
    """Initializes the SQLite database table if it doesn't exist."""
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS predictions_history (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER,
            league TEXT,
            pred_type TEXT,
            match_name TEXT,
            selection TEXT,
            bookie TEXT,
            odds REAL,
            edge REAL,
            timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
        )
    """)
    conn.commit()
    conn.close()

def save_prediction_if_new(user_id: int, league: str, pred_type: str, match_name: str, selection: str, bookie: str, odds: float, edge: float):
    """Saves a prediction into SQLite only if it hasn't been logged today."""
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    
    cursor.execute("""
        SELECT id FROM predictions_history 
        WHERE user_id = ? AND match_name = ? AND selection = ? AND DATE(timestamp) = DATE('now')
    """, (user_id, match_name, selection))
    
    if not cursor.fetchone():
        cursor.execute("""
            INSERT INTO predictions_history (user_id, league, pred_type, match_name, selection, bookie, odds, edge)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, (user_id, league, pred_type, match_name, selection, bookie, odds, edge))
        conn.commit()
        
    conn.close()

def get_recent_history(user_id: int, limit: int = 8) -> List[Dict[str, Any]]:
    """Retrieves recent prediction history for a user."""
    conn = sqlite3.connect(DB_NAME)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    cursor.execute("""
        SELECT league, pred_type, match_name, selection, bookie, odds, edge, timestamp
        FROM predictions_history
        WHERE user_id = ?
        ORDER BY id DESC
        LIMIT ?
    """, (user_id, limit))
    rows = cursor.fetchall()
    conn.close()
    return [dict(row) for row in rows]

# Initialize DB on startup
init_db()

# -------------------------------------------------------------------
# API CACHE (Prevents duplicate requests & conserves API quota)
# -------------------------------------------------------------------
ODDS_CACHE: Dict[str, Dict[str, Any]] = {}
CACHE_TTL = 300  # 5 Minutes cache duration

SUPPORTED_SPORTS = {
    "soccer_epl": "⚽ Premier League",
    "soccer_uefa_champs_league": "🇪🇺 Champions League",
    "soccer_spain_la_liga": "🇪🇸 La Liga",
    "soccer_germany_bundesliga": "🇩🇪 Bundesliga",
    "basketball_nba": "🏀 NBA Basketball",
    "americanfootball_nfl": "🏈 NFL Football"
}

def get_user_config(user_id: int) -> dict:
    if user_id not in USER_SETTINGS:
        USER_SETTINGS[user_id] = {"sport": "soccer_epl", "min_ev": 0.015}
    return USER_SETTINGS[user_id]

# -------------------------------------------------------------------
# Cached Odds Fetcher
# -------------------------------------------------------------------
async def fetch_live_odds_cached(sport_key: str) -> List[Dict[str, Any]]:
    now = time.time()
    
    if sport_key in ODDS_CACHE:
        cached_entry = ODDS_CACHE[sport_key]
        if now - cached_entry["timestamp"] < CACHE_TTL:
            logging.info(f"⚡ [CACHE HIT] Returning saved odds for {sport_key}")
            return cached_entry["data"]

    if not ODDS_API_KEY:
        logging.error("ODDS_API_KEY missing from environment!")
        return []

    regions = "us" if "nba" in sport_key or "nfl" in sport_key else "uk,eu"
    url = f"{BASE_URL}/{sport_key}/odds/"
    params = {
        "apiKey": ODDS_API_KEY,
        "regions": regions,
        "markets": "h2h",
        "oddsFormat": "decimal",
    }
    
    logging.info(f"🌐 [API CALL] Requesting live odds for {sport_key}...")
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(url, params=params, timeout=12) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    ODDS_CACHE[sport_key] = {"timestamp": now, "data": data}
                    return data
                else:
                    logging.error(f"API Error [{resp.status}]: {await resp.text()}")
                    return []
    except Exception as e:
        logging.error(f"Exception fetching odds: {e}")
        return []

# -------------------------------------------------------------------
# Telegram User Interface & Keyboards
# -------------------------------------------------------------------
def build_quick_menu(config: dict) -> InlineKeyboardMarkup:
    keyboard = [
        [
            InlineKeyboardButton("🎯 Scan +EV Bets", callback_data="run_ev"),
            InlineKeyboardButton("⚡ Scan Surebets", callback_data="run_arb")
        ],
        [
            InlineKeyboardButton("📜 View History Log", callback_data="run_history")
        ],
        [
            InlineKeyboardButton("⚽ EPL", callback_data="sport_soccer_epl"),
            InlineKeyboardButton("🇪🇺 UCL", callback_data="sport_soccer_uefa_champs_league"),
            InlineKeyboardButton("🏀 NBA", callback_data="sport_basketball_nba")
        ],
        [
            InlineKeyboardButton("🇪🇸 La Liga", callback_data="sport_soccer_spain_la_liga"),
            InlineKeyboardButton("🇩🇪 Bundesliga", callback_data="sport_soccer_germany_bundesliga")
        ]
    ]
    return InlineKeyboardMarkup(keyboard)

# -------------------------------------------------------------------
# Telegram Command & Callback Handlers
# -------------------------------------------------------------------
async def start_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    config = get_user_config(update.effective_user.id)
    sport_name = SUPPORTED_SPORTS.get(config["sport"], "⚽ Premier League")
    
    await update.message.reply_text(
        f"🚀 *Sports Analytics Hub*\n\n"
        f"Active League: *{sport_name}*\n"
        f"Select an operation below to view market edges or check your prediction history:",
        parse_mode="Markdown",
        reply_markup=build_quick_menu(config)
    )

async def history_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg = get_history_summary(update.effective_user.id)
    config = get_user_config(update.effective_user.id)
    await update.message.reply_text(msg, parse_mode="Markdown", reply_markup=build_quick_menu(config))

async def button_router(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    user_id = query.from_user.id
    config = get_user_config(user_id)
    data = query.data

    if data.startswith("sport_"):
        sport_key = data.replace("sport_", "")
        config["sport"] = sport_key
        sport_name = SUPPORTED_SPORTS.get(sport_key, sport_key)
        await query.edit_message_text(
            f"✅ League changed to *{sport_name}*\nChoose scan mode:",
            parse_mode="Markdown",
            reply_markup=build_quick_menu(config)
        )

    elif data == "run_ev":
        await query.edit_message_text("🔎 *Fetching odds and calculating power-model fair probabilities...*", parse_mode="Markdown")
        msg = await generate_all_ev_predictions(user_id, config["sport"], config["min_ev"])
        await query.edit_message_text(msg, parse_mode="Markdown", reply_markup=build_quick_menu(config))

    elif data == "run_arb":
        await query.edit_message_text("⚡ *Checking cross-bookmaker arbitrage matches...*", parse_mode="Markdown")
        msg = await generate_all_arb_predictions(user_id, config["sport"])
        await query.edit_message_text(msg, parse_mode="Markdown", reply_markup=build_quick_menu(config))

    elif data == "run_history":
        msg = get_history_summary(user_id)
        await query.edit_message_text(msg, parse_mode="Markdown", reply_markup=build_quick_menu(config))

# -------------------------------------------------------------------
# History Summary Generator
# -------------------------------------------------------------------
def get_history_summary(user_id: int) -> str:
    history = get_recent_history(user_id, limit=8)
    if not history:
        return "📜 *Prediction History*\n\nNo saved predictions found yet. Run a scan to populate history."

    output = "📜 *Recent Prediction History*\n"
    output += "═════════════════════\n\n"

    for h in history:
        date_str = h['timestamp'].split()[0] if ' ' in h['timestamp'] else h['timestamp']
        type_icon = "🎯" if h['pred_type'] == "+EV" else "⚡"
        output += (
            f"{type_icon} *{h['match_name']}* ({h['league']})\n"
            f"👉 *Selection:* `{h['selection']}` @ *{h['odds']}* ({h['bookie']})\n"
            f"📈 *Edge:* `+{h['edge']}%` | 🗓 *Date:* `{date_str}`\n\n"
        )
    return output

# -------------------------------------------------------------------
# Core Analytics Engine Logic (Power-Method Devigging Engine)
# -------------------------------------------------------------------
async def generate_all_ev_predictions(user_id: int, sport_key: str, min_ev: float) -> str:
    matches = await fetch_live_odds_cached(sport_key)
    league_name = SUPPORTED_SPORTS.get(sport_key, "Selected League")

    if not matches:
        return f"ℹ️ *No active games or odds found right now for {league_name}.*"

    signals = []
    for match in matches:
        bookies = match.get("bookmakers", [])
        if len(bookies) < 2:
            continue

        home_team = match.get("home_team")
        away_team = match.get("away_team")

        # 1. Identify sharp baseline bookie (Pinnacle/Unibet preferred)
        sharp = next((b for b in bookies if b["key"] in ["pinnacle", "unibet_eu"]), bookies[0])
        sharp_h2h = next((m for m in sharp.get("markets", []) if m["key"] == "h2h"), None)
        if not sharp_h2h:
            continue

        sharp_outcomes = sharp_h2h.get("outcomes", [])
        if not sharp_outcomes:
            continue

        sharp_prices = [o["price"] for o in sharp_outcomes]
        
        # Power-method de-vigging handles longshots & favorite-longshot bias correctly
        fair_probs_list = devig_power_method(sharp_prices)

        if not fair_probs_list or len(fair_probs_list) != len(sharp_outcomes):
            continue

        fair_prob_map = {out["name"]: fair_probs_list[idx] for idx, out in enumerate(sharp_outcomes)}

        # 2. Check soft bookies against corrected sharp fair probability
        for soft in bookies:
            if soft["key"] == sharp["key"]:
                continue

            soft_h2h = next((m for m in soft.get("markets", []) if m["key"] == "h2h"), None)
            if not soft_h2h:
                continue

            for outcome in soft_h2h.get("outcomes", []):
                name = outcome["name"]
                soft_price = outcome["price"]

                if name in fair_prob_map:
                    fair_p = fair_prob_map[name]
                    ev = calculate_ev(fair_p, soft_price)

                    # Strict filter: Genuine sharp +EV edges stay between +1.5% and +8.0%
                    if 0.015 <= ev <= 0.08:
                        kelly = calculate_kelly_stake(fair_p, soft_price)
                        signals.append({
                            "match": f"{home_team} vs {away_team}",
                            "selection": name,
                            "bookie": soft["title"],
                            "odds": soft_price,
                            "fair_odds": round(1.0 / fair_p, 2),
                            "ev": round(ev * 100, 1),
                            "stake": round(kelly * 100, 1)
                        })

    if not signals:
        return f"ℹ️ *No valid +EV edges found within realistic threshold (+1.5% to +8.0%) for {league_name}.*"

    # Match Deduplication: Keep ONLY the single top EV pick per match
    best_match_signals = {}
    for sig in sorted(signals, key=lambda x: x["ev"], reverse=True):
        match_key = sig["match"]
        if match_key not in best_match_signals:
            best_match_signals[match_key] = sig

    # Save unique predictions to history
    for s in best_match_signals.values():
        save_prediction_if_new(
            user_id=user_id,
            league=league_name,
            pred_type="+EV",
            match_name=s["match"],
            selection=s["selection"],
            bookie=s["bookie"],
            odds=s["odds"],
            edge=s["ev"]
        )

    output = f"🎯 *+EV Value Predictions overview*\n"
    output += f"🏆 *League:* {league_name}\n"
    output += f"📊 *Matches Found:* {len(best_match_signals)}\n"
    output += "═════════════════════\n\n"

    for s in best_match_signals.values():
        output += (
            f"🏟 *{s['match']}*\n"
            f"👉 *Bet:* `{s['selection']}` @ *{s['odds']}* ({s['bookie']})\n"
            f"📈 *Edge:* `+{s['ev']}% EV` (Fair: {s['fair_odds']})\n"
            f"💰 *Rec Stake:* `{s['stake']}% bankroll`\n\n"
        )

    return output

async def generate_all_arb_predictions(user_id: int, sport_key: str) -> str:
    matches = await fetch_live_odds_cached(sport_key)
    league_name = SUPPORTED_SPORTS.get(sport_key, "Selected League")

    if not matches:
        return f"ℹ️ *No active games found for {league_name}.*"

    arbs = []
    for match in matches:
        home = match.get("home_team")
        away = match.get("away_team")
        bookies = match.get("bookmakers", [])

        if len(bookies) < 2:
            continue

        best_outcomes = {}
        for b in bookies:
            h2h = next((m for m in b.get("markets", []) if m["key"] == "h2h"), None)
            if not h2h:
                continue

            for out in h2h.get("outcomes", []):
                name = out["name"]
                price = out["price"]

                if name not in best_outcomes or price > best_outcomes[name]["odds"]:
                    best_outcomes[name] = {"name": name, "odds": price, "bookmaker": b["title"]}

        outcomes_list = list(best_outcomes.values())
        arb = scan_match_arbitrage(outcomes_list, total_stake=100.0)

        if arb and arb["profit_margin_pct"] > 0.0:
            arbs.append({
                "match": f"{home} vs {away}",
                "margin": arb["profit_margin_pct"],
                "profit": arb["guaranteed_profit"],
                "details": arb["details"],
                "stakes": arb["stakes"]
            })

    if not arbs:
        return f"ℹ️ *No Surebet Arbitrage opportunities found right now for {league_name}.*"

    for a in arbs:
        save_prediction_if_new(
            user_id=user_id,
            league=league_name,
            pred_type="Arbitrage",
            match_name=a["match"],
            selection="Cross-Bookie Surebet",
            bookie="Multi-Bookie",
            odds=1.0,
            edge=a["margin"]
        )

    output = f"⚡ *Guaranteed Arbitrage overview*\n"
    output += f"🏆 *League:* {league_name}\n"
    output += "═════════════════════\n\n"

    for a in arbs:
        output += f"🏟 *{a['match']}*\n"
        output += f"📈 *Profit:* `+{a['margin']}% (${a['profit']} / $100 bet)`\n"
        for i, d in enumerate(a["details"]):
            output += f"  • Put *${a['stakes'][i]}* on `{d['name']}` @ *{d['odds']}* ({d['bookmaker']})\n"
        output += "\n"

    return output

# -------------------------------------------------------------------
# Application Entry Point
# -------------------------------------------------------------------
def main():
    if not TELEGRAM_TOKEN:
        raise ValueError("TELEGRAM_BOT_TOKEN missing from .env file!")

    app = ApplicationBuilder().token(TELEGRAM_TOKEN).build()
    app.add_handler(CommandHandler("start", start_command))
    app.add_handler(CommandHandler("history", history_command))
    app.add_handler(CallbackQueryHandler(button_router))

    print("⚡ Bot is running with Power-Method Devigging & strict EV limits!")
    app.run_polling()

if __name__ == "__main__":
    main()