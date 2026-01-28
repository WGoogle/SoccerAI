import os
from dotenv import load_dotenv
from datatry import ComprehensiveSoccerDataIngestion

# Getting the API Key
load_dotenv()
api_key = os.getenv("API_key")

# Initializing the data from ingestion pipeline
ingestion = ComprehensiveSoccerDataIngestion(api_key = api_key)

# Giving it a test with Haaland's id and season, this will only use on API Call just for good test
#data = ingestion.get_player_stats(player_id = 1100, season = 2023)

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
# Checking leagues
# leagues = ingestion.get_leagues(country="England") 

"""
# Checking games
fixtures = ingestion.get_fixtures(league=39, season=2024, next=10)

# Checking today's games
from datetime import datetime
today = datetime.now().strftime("%Y-%m-%d")
fixtures = ingestion.get_fixtures(date=today)
print(fixtures)

"""

# Checking standings 
"""
standings = ingestion.get_standings(league=39, season=2024)
print(standings[["rank", "team_name", "points", "form"]])
"""

# CHecking team statistics
"""
stats = ingestion.get_teams_statistics(league=39, season=2024, team=33)  # Man Yanited
print(f"Form: {stats['form']}")
print(f"Goals scored: {stats['goals_for_total']}")
print(f"Yellow cards: {stats['yellow_cards_total']}")
"""
import json
# CHecking head-to-head (Man Yanited vs Liverpool)
h2h = ingestion.get_h2h(team1_id=40, team2_id=42, season=2024)
print(f"Found {len(h2h)} H2H matches")
if not h2h.empty:
    print(h2h[['date', 'home_team_name', 'away_team_name', 'home_goals', 'away_goals', 'winner']])
