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

# 45+ Active Worldwide Leagues to prevent match starvation on quiet days
GLOBAL_MASSIVE_LEAGUES = {
    # England
    "soccer_epl": "Premier League (ENG)",
    "soccer_efl_champ": "Championship (ENG)",
    "soccer_england_league1": "League One (ENG)",
    "soccer_england_league2": "League Two (ENG)",
    "soccer_england_efl_cup": "EFL Cup (ENG)",
    "soccer_fa_cup": "FA Cup (ENG)",

    # Spain
    "soccer_spain_la_liga": "La Liga (ESP)",
    "soccer_spain_segunda_division": "Segunda División (ESP)",
    "soccer_spain_copa_del_rey": "Copa del Rey (ESP)",

    # Germany
    "soccer_germany_bundesliga": "Bundesliga (GER)",
    "soccer_germany_bundesliga2": "2. Bundesliga (GER)",
    "soccer_germany_liga3": "3. Liga (GER)",
    "soccer_germany_dfb_pokal": "DFB-Pokal (GER)",

    # Italy
    "soccer_italy_serie_a": "Serie A (ITA)",
    "soccer_italy_serie_b": "Serie B (ITA)",
    "soccer_italy_coppa_italia": "Coppa Italia (ITA)",

    # France
    "soccer_france_ligue_one": "Ligue 1 (FRA)",
    "soccer_france_ligue_two": "Ligue 2 (FRA)",

    # UEFA Continental Tournaments
    "soccer_uefa_champs_league": "UEFA Champions League",
    "soccer_uefa_europa_league": "UEFA Europa League",
    "soccer_uefa_conference_league": "UEFA Conference League",

    # Top European Competitions
    "soccer_netherlands_eredivisie": "Eredivisie (NED)",
    "soccer_netherlands_eerste_divisie": "Eerste Divisie (NED)",
    "soccer_portugal_primeira_liga": "Primeira Liga (POR)",
    "soccer_belgium_first_div": "Pro League (BEL)",
    "soccer_turkey_super_league": "Süper Lig (TUR)",
    "soccer_scotland_premiership": "Premiership (SCO)",
    "soccer_scotland_championship": "Championship (SCO)",
    "soccer_greece_super_league": "Super League (GRE)",
    "soccer_austria_bundesliga": "Bundesliga (AUT)",
    "soccer_switzerland_superleague": "Super League (SUI)",
    "soccer_denmark_superliga": "Superliga (DEN)",
    "soccer_poland_ekstraklasa": "Ekstraklasa (POL)",
    "soccer_sweden_allsvenskan": "Allsvenskan (SWE)",
    "soccer_norway_eliteserien": "Eliteserien (NOR)",

    # Americas
    "soccer_brazil_campeonato": "Série A (BRA)",
    "soccer_brazil_serie_b": "Série B (BRA)",
    "soccer_brazil_copa": "Copa do Brasil (BRA)",
    "soccer_argentina_primera_division": "Primera (ARG)",
    "soccer_argentina_copa": "Copa Argentina (ARG)",
    "soccer_mexico_ligamx": "Liga MX (MEX)",
    "soccer_usa_mls": "MLS (USA)",
    "soccer_usa_usl_championship": "USL (USA)",
    "soccer_chile_camp_nacional": "Primera (CHI)",
    "soccer_colombia_primera_a": "Primera A (COL)",

    # Asia & Oceania
    "soccer_japan_j_league": "J1 League (JPN)",
    "soccer_korea_kleague1": "K League 1 (KOR)",
    "soccer_australia_aleague": "A-League (AUS)",
    "soccer_saudi_arabia_pro_league": "Pro League (KSA)"
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
# Multi-Market Projection Engine (Tuned for 1.40 - 1.75 Value Legs)
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

    # 1. Straight Match Winner (Favorites between 1.40 and 1.80)
    if home_p >= 0.50 and 1.40 <= f["home_odds"] <= 1.80:
        candidates.append({
            "pick": f"{home_name} to Win",
            "category": "🏆 1X2",
            "odds": f["home_odds"],
            "prob": home_p
        })
    elif away_p >= 0.50 and 1.40 <= f["away_odds"] <= 1.80:
        candidates.append({
            "pick": f"{away_name} to Win",
            "category": "🏆 1X2",
            "odds": f["away_odds"],
            "prob": away_p
        })

    # 2. Over 1.5 & Over 2.5 Total Match Goals
    prob_over_1_5 = sum(
        poisson_prob(home_xg, h) * poisson_prob(away_xg, a)
        for h in range(6) for a in range(6) if h + a > 1.5
    )
    if prob_over_1_5 >= 0.68:
        est_odds = round(1.0 / (prob_over_1_5 * 1.05), 2)
        if 1.35 <= est_odds <= 1.65:
            candidates.append({
                "pick": "Over 1.5 Goals",
                "category": "⚽ Goals",
                "odds": est_odds,
                "prob": prob_over_1_5
            })

    prob_over_2_5 = sum(
        poisson_prob(home_xg, h) * poisson_prob(away_xg, a)
        for h in range(6) for a in range(6) if h + a > 2.5
    )
    if prob_over_2_5 >= 0.54:
        est_odds = round(1.0 / (prob_over_2_5 * 1.05), 2)
        if 1.45 <= est_odds <= 1.82:
            candidates.append({
                "pick": "Over 2.5 Goals",
                "category": "⚽ Goals",
                "odds": est_odds,
                "prob": prob_over_2_5
            })

    # 3. Both Teams To Score (GG Yes)
    prob_btts = (1.0 - poisson_prob(home_xg, 0)) * (1.0 - poisson_prob(away_xg, 0))
    if prob_btts >= 0.56:
        est_odds = round(1.0 / (prob_btts * 1.05), 2)
        if 1.42 <= est_odds <= 1.78:
            candidates.append({
                "pick": "Both Teams To Score (GG)",
                "category": "⚽ Goals",
                "odds": est_odds,
                "prob": prob_btts
            })

    # 4. Corners & Booking Cards
    est_corners = 8.5 + (total_xg * 0.95)
    if est_corners >= 10.0:
        prob_over_8_5_c = min(0.82, 0.50 + (est_corners - 9.5) * 0.12)
        est_odds = round(1.0 / (prob_over_8_5_c * 1.06), 2)
        if 1.35 <= est_odds <= 1.65:
            candidates.append({
                "pick": "Corners Over 8.5",
                "category": "🚩 Corners",
                "odds": est_odds,
                "prob": prob_over_8_5_c
            })

    parity = 1.0 - abs(home_p - away_p)
    est_cards = 3.2 + (parity * 1.6)
    if est_cards >= 4.2:
        prob_over_3_5_cards = min(0.80, 0.48 + (est_cards - 4.0) * 0.14)
        est_odds = round(1.0 / (prob_over_3_5_cards * 1.06), 2)
        if 1.35 <= est_odds <= 1.65:
            candidates.append({
                "pick": "Cards Over 3.5",
                "category": "🟨 Cards",
                "odds": est_odds,
                "prob": prob_over_3_5_cards
            })

    # 5. Value Double Chance (Filtering out non-viable 1.15 odds)
    p_1x = home_p + draw_p
    if 0.64 <= p_1x <= 0.77:
        est_odds = round(1.0 / (p_1x * 1.05), 2)
        if 1.32 <= est_odds <= 1.55:
            candidates.append({
                "pick": f"{home_name} or Draw (1X)",
                "category": "🛡️ Double Chance",
                "odds": est_odds,
                "prob": p_1x
            })

    p_x2 = away_p + draw_p
    if 0.64 <= p_x2 <= 0.77:
        est_odds = round(1.0 / (p_x2 * 1.05), 2)
        if 1.32 <= est_odds <= 1.55:
            candidates.append({
                "pick": f"Draw or {away_name} (X2)",
                "category": "🛡️ Double Chance",
                "odds": est_odds,
                "prob": p_x2
            })

    if not candidates:
        return None

    # Pick selection with highest risk-adjusted expected value
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
# Module 1: READ (48-60h Horizon across 45+ Leagues)
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

    return {"success": False, "message": "Zero active fixtures found within the rolling horizon."}

# -------------------------------------------------------------------
# Module 2: PREDICT (Compact Low-Team Count Delivery Engine)
# -------------------------------------------------------------------
async def generate_dynamic_slip(target_odds: float, max_odds: float, max_legs: int) -> str:
    cached_matches = await load_future_fixtures()
    if not cached_matches:
        return "⚠️ *Database empty.* Tap **📖 Read 48h Matches (Store DB)** first.[span_0](start_span)"[span_0](end_span)

    evaluated = []
    for f in cached_matches:
        cand = select_best_dynamic_market(f)
        if cand:
            evaluated.append(cand)

    # Sort descending by calculated probability
    evaluated.sort(key=lambda x: x["prob"], reverse=True)

    selected_legs = []
    league_counts = {}
    current_odds = 1.0

    for leg in evaluated:
        l_name = leg["league_name"]
        # Allow up to 2 matches per league to avoid artificial match starvation
        if league_counts.get(l_name, 0) >= 2:
            continue

        if len(selected_legs) >= max_legs:
            break

        if (current_odds * leg["odds"]) > (max_odds * 1.20):
            continue

        selected_legs.append(leg)
        league_counts[l_name] = league_counts.get(l_name, 0) + 1
        current_odds *= leg["odds"]

        if current_odds >= target_odds:
            break

    if len(selected_legs) < 2:
        return (
            "⚠️ *Not enough qualifying matches for this ticket right now.*\n\n"
            "Tap **📖 Read 48h Matches** later as bookmakers post new lines."
        )

    target_label = int(target_odds)
    report = [
        f"🎯 *COMPACT {target_label} ODDS SLIP ({len(selected_legs)} TEAMS ONLY)*",
        f"⏱️ Window: `Today & Tomorrow`",
        f"📈 Total Odds: `{current_odds:.2f}`",
        f"🔒 Total Matches: `{len(selected_legs)} Games`",
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
    report.append("💡 *Compact format active. Staking rule: 0.5% – 1% bankroll unit.*")
    return "\n".join(report)

# -------------------------------------------------------------------
# Telegram Keyboards & Router
# -------------------------------------------------------------------
def build_main_keyboard(cached_count: int) -> InlineKeyboardMarkup:
    keyboard = [
        [InlineKeyboardButton("📖 Read 48h Matches (Store DB)", callback_data="btn_read")],
        [
            InlineKeyboardButton("🎯 5 Odds (3-4)", callback_data="pred_5"),
            InlineKeyboardButton("🔥 10 Odds (5-6)", callback_data="pred_10"),
            InlineKeyboardButton("🚀 20 Odds (7-8)", callback_data="pred_20")
        ]
    ]
    return InlineKeyboardMarkup(keyboard)

async def start_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await init_db()
    count = await get_cached_fixtures_count()
    await update.message.reply_text(
        f"⚽ *Compact Odds Accumulator Engine*\n\n"
        f"📊 *Cached Matches Available:* `{count}`\n\n"
        f"• **📖 Read 48h Matches**: Ingests upcoming fixtures across 45+ worldwide competitions into SQLite.\n"
        f"• **Compact Policy**: 5 odds (3–4 teams), 10 odds (5–6 teams), and 20 odds (7–8 teams).\n\n"
        f"Select your odds preference below:",
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
            text="⏳ *Scanning 45+ global leagues for today & tomorrow's matches...*",
            parse_mode="Markdown"
        )
        res = await run_read_and_store_pipeline()
        await status.delete()

        count = await get_cached_fixtures_count()
        if res.get("success"):
            text = (
                f"✅ *Matches Synchronized!*\n\n"
                f"Indexed `{res['count']}` fixtures across `{res['leagues']}` competitions into SQLite.\n\n"
                f"Select your desired odds slip below:"
            )
        else:
            text = f"⚠️ *Notice:* {res.get('message')}"

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

        config_map = {
            "pred_5": (5.0, 7.5, 4),
            "pred_10": (10.0, 14.0, 6),
            "pred_20": (20.0, 26.0, 8)
        }
        target, maximum, max_legs = config_map[data]

        status = await context.bot.send_message(
            chat_id=chat_id,
            text=f"⚙️ *Building compact ticket with max {max_legs} teams for ~{int(target)} odds...*",
            parse_mode="Markdown"
        )
        report = await generate_dynamic_slip(target_odds=target, max_odds=maximum, max_legs=max_legs)
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

    print("🚀 Bot active with 45+ league discovery and compact parlay builder...")
    app.run_polling(drop_pending_updates=True)

if __name__ == "__main__":
    main()
