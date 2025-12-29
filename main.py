import os
from dotenv import load_dotenv
from datatry import ComprehensiveSoccerDataIngestion

# Getting the API Key
load_dotenv()
api_key = os.getenv("API_key")

# Initializing the data from ingestion pipeline
ingestion = ComprehensiveSoccerDataIngestion(api_key = api_key)

# Giving it a test with Haaland's id and season, this will only use on API Call just for good test
data = ingestion.get_player_stats(player_id = 1100, season = 2023)

# Verifying it is correct
"""
if data.get("response"):
    stats = data["response"][0]["statistics"][0]
    team_name = stats["team"]["name"]
    goals = stats["goals"]["total"]
    print(f"Team: {team_name}")
    print(f"Goals: {goals}")
else:
    print("No data found. Uh oh...")

"""
ingestion.get_leagues(country="England") 