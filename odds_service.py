import os
import aiohttp
import logging
from typing import List, Dict, Any
from dotenv import load_dotenv

# Load environment variables from .env
load_dotenv()

ODDS_API_KEY = os.getenv("ODDS_API_KEY")
BASE_URL = "https://api.the-odds-api.com/v4/sports"

async def get_active_sports() -> List[Dict[str, Any]]:
    """Fetch list of currently active sports leagues."""
    if not ODDS_API_KEY:
        logging.error("ODDS_API_KEY missing from .env file!")
        return []

    url = f"{BASE_URL}?apiKey={ODDS_API_KEY}"
    async with aiohttp.ClientSession() as session:
        async with session.get(url) as resp:
            if resp.status == 200:
                return await resp.json()
            else:
                logging.error(f"Error fetching sports: {resp.status}")
                return []

async def fetch_live_odds(sport_key: str = "soccer_epl", regions: str = "uk,eu,us") -> List[Dict[str, Any]]:
    """
    Fetch upcoming match odds for a given sport key.
    Default: Premier League (soccer_epl) across major bookies.
    """
    if not ODDS_API_KEY:
        logging.error("ODDS_API_KEY missing from .env file!")
        return []

    url = f"{BASE_URL}/{sport_key}/odds/"
    params = {
        "apiKey": ODDS_API_KEY,
        "regions": regions,
        "markets": "h2h",
        "oddsFormat": "decimal",
    }
    
    async with aiohttp.ClientSession() as session:
        async with session.get(url, params=params) as resp:
            if resp.status == 200:
                return await resp.json()
            else:
                logging.error(f"Failed to fetch odds [{resp.status}]: {await resp.text()}")
                return []