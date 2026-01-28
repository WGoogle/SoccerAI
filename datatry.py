import time
import requests
import os
import pandas as pd

class ComprehensiveSoccerDataIngestion:
    
    def __init__(self, api_key, rate_limit_delay: float = 12.0): 
        self.api_key = api_key
        self.base_url = "https://v3.football.api-sports.io"
        self.headers = {
            "x-apisports-key": api_key,
        }
        self.cache_dir = "soccer_data_cache"
        self.rate_limit_delay = rate_limit_delay  # Worried about being on free plan for API
        os.makedirs(self.cache_dir, exist_ok=True)
        
        # Ensuring I got API connection
        # self.test_connection()
    
    def test_connection(self):
        try:
            response = requests.get(
                f"{self.base_url}/timezone",
                headers=self.headers
            )
            if response.status_code == 401:
                raise ValueError("API key is invalid. Check the RapidAPI key.") #ran into this issue, fixed with website document
            elif response.status_code == 429:
                print("Rate limit, need to be increasing delays.") # On free plan so making sure i dont get banned
            else:
                response.raise_for_status()
                print("API connection successful!\n")
        except requests.exceptions.RequestException as e:
            print(f"API connection failed: {e}")
            raise
        
    def api_call(self, endpoint, params, retries: int = 3):
        for attempt in range(retries):
            try:
                response = requests.get(
                    f"{self.base_url}/{endpoint}", 
                    headers=self.headers, 
                    params=params,
                    timeout=10
                )
                
                if response.status_code == 429: # once again just ensuring i do not get banned
                    wait_time = (attempt + 1) * 5
                    print(f" Rate limit gotten so waiting {wait_time}s before retry")
                    time.sleep(wait_time)
                    continue
                
                response.raise_for_status() #good saftey check

                
                # By this point, the call was successful, so now delay the next call for rate limit prevention
                time.sleep(self.rate_limit_delay)

                remaining = response.headers.get("x-ratelimit-requests-remaining")
                limit = response.headers.get("x-ratelimit-requests-limit")
                
 
                if remaining and limit:
                    remaining_int = int(remaining)
                    limit_int = int(limit)
                    print(f"API Quota: {remaining_int}/{limit_int} remaining today.") # A visual representation of API Calls left too
                    
                    if remaining_int < 2:
                        print("STOP IT: Daily quota reached. Need to take that 24 hr break unfortunately.")
                        raise SystemExit("Daily Quota almost surpassed")
                
                result = response.json()
                return result
                              
            except requests.exceptions.Timeout:
                print(f"Request timeout on {endpoint} (attempt {attempt + 1}/{retries})")
                if attempt < retries - 1:
                    time.sleep(2)
                    continue
                    
            except requests.exceptions.RequestException as e:
                print(f"API Error on {endpoint}: {e}")
                if attempt < retries - 1:
                    time.sleep(2)
                    continue
        
        return {}
    
    def get_player_stats(self, player_id, season):
        """
        Will likely update in future.
        Fetches stats for a specific player in a specific season.
        """
        print(f"Fetching stats for Player ID: {player_id}, Season: {season}...")
        
        # This endpoint returns all stats for one player in one season ; 1 request 
        params = {
            "id": player_id,
            "season": season
        }
        
        return self.api_call("players", params)
    
    # season check
    
    def get_seasons(self):
        """Get all available seasons"""
        data = self.api_call("seasons", {})
        return data.get("response", [])
    

    # leagues check
    def get_leagues(self, id: int = None, name: str = None, country: str = None, season: int = None, team: int = None, type: str = None):
        """
        Get leagues with all the available data.
        """
        params = {}
        if id is not None: params["id"] = id
        if name: params["name"] = name
        if country: params["country"] = country
        if season: params["season"] = season
        if team: params["team"] = team
        if type: params["type"] = type
            
        data = self.api_call("leagues", params)
        
        # Dataframe for leagues endpoint
        return pd.DataFrame([
            {
                "league_id": item.get("league", {}).get("id"),
                "name": item.get("league", {}).get("name"),
                "team": item.get("league", {}).get("team"),
                "type": item.get("league", {}).get("type")
            }
            for item in data.get("response", [])
        ])
    
    def get_standings(self, league: int, season: int):
        params = {
        "league": league,
        "season": season
        }
    
        data = self.api_call("standings", params)
    
        # Standings are nested so need to do some for loops
        standings_list = []
        for resp in data.get("response", []):
            for standing_group in resp.get("league", {}).get("standings", []):
                for team in standing_group:
                    standings_list.append({
                    "rank": team.get("rank"),
                    "team_id": team.get("team", {}).get("id"),
                    "team_name": team.get("team", {}).get("name"),
                    "team_logo": team.get("team", {}).get("logo"),
                    "points": team.get("points"),
                    "goalsDiff": team.get("goalsDiff"),
                    "group": team.get("group"),
                    "form": team.get("form"),
                    "status": team.get("status"),
                    "description": team.get("description"),
                    "played": team.get("all", {}).get("played"),
                    "win": team.get("all", {}).get("win"),
                    "draw": team.get("all", {}).get("draw"),
                    "lose": team.get("all", {}).get("lose"),
                    "goals_for": team.get("all", {}).get("goals", {}).get("for"),
                    "goals_against": team.get("all", {}).get("goals", {}).get("against"),
                    "home_played": team.get("home", {}).get("played"),
                    "home_win": team.get("home", {}).get("win"),
                    "home_draw": team.get("home", {}).get("draw"),
                    "home_lose": team.get("home", {}).get("lose"),
                    "away_played": team.get("away", {}).get("played"),
                    "away_win": team.get("away", {}).get("win"),
                    "away_draw": team.get("away", {}).get("draw"),
                    "away_lose": team.get("away", {}).get("lose")
                })
    
        return pd.DataFrame(standings_list)

    def get_fixtures(self, id: int = None, date: str = None, league: int = None, season: int = None, team: int = None, last: int = None, 
                next: int = None, from_date: str = None, to: str = None, round: str = None, status: str = None, timezone: str = None):
        params = {}
        if id is not None: params["id"] = id
        if date: params["date"] = date
        if league is not None: params["league"] = league
        if season is not None: params["season"] = season
        if team is not None: params["team"] = team
        if last is not None: params["last"] = last
        if next is not None: params["next"] = next
        if from_date: params["from"] = from_date
        if to: params["to"] = to
        if round: params["round"] = round
        if status: params["status"] = status
        if timezone: params["timezone"] = timezone
    
        data = self.api_call("fixtures", params)
    
        return pd.DataFrame([
        {
            "fixture_id": item.get("fixture", {}).get("id"),
            "referee": item.get("fixture", {}).get("referee"),
            "timezone": item.get("fixture", {}).get("timezone"),
            "date": item.get("fixture", {}).get("date"),
            "timestamp": item.get("fixture", {}).get("timestamp"),
            "venue_id": item.get("fixture", {}).get("venue", {}).get("id"),
            "venue_name": item.get("fixture", {}).get("venue", {}).get("name"),
            "venue_city": item.get("fixture", {}).get("venue", {}).get("city"),
            "status_long": item.get("fixture", {}).get("status", {}).get("long"),
            "status_short": item.get("fixture", {}).get("status", {}).get("short"),
            "status_elapsed": item.get("fixture", {}).get("status", {}).get("elapsed"),
            "league_id": item.get("league", {}).get("id"),
            "league_name": item.get("league", {}).get("name"),
            "league_country": item.get("league", {}).get("country"),
            "league_season": item.get("league", {}).get("season"),
            "league_round": item.get("league", {}).get("round"),
            "home_team_id": item.get("teams", {}).get("home", {}).get("id"),
            "home_team_name": item.get("teams", {}).get("home", {}).get("name"),
            "home_team_logo": item.get("teams", {}).get("home", {}).get("logo"),
            "home_team_winner": item.get("teams", {}).get("home", {}).get("winner"),
            "away_team_id": item.get("teams", {}).get("away", {}).get("id"),
            "away_team_name": item.get("teams", {}).get("away", {}).get("name"),
            "away_team_logo": item.get("teams", {}).get("away", {}).get("logo"),
            "away_team_winner": item.get("teams", {}).get("away", {}).get("winner"),
            "home_goals": item.get("goals", {}).get("home"),
            "away_goals": item.get("goals", {}).get("away"),
            "halftime_home": item.get("score", {}).get("halftime", {}).get("home"),
            "halftime_away": item.get("score", {}).get("halftime", {}).get("away"),
            "fulltime_home": item.get("score", {}).get("fulltime", {}).get("home"),
            "fulltime_away": item.get("score", {}).get("fulltime", {}).get("away"),
            "extratime_home": item.get("score", {}).get("extratime", {}).get("home"),
            "extratime_away": item.get("score", {}).get("extratime", {}).get("away"),
            "penalty_home": item.get("score", {}).get("penalty", {}).get("home"),
            "penalty_away": item.get("score", {}).get("penalty", {}).get("away")
        }
        for item in data.get("response", [])
    ])

    def get_teams_statistics(self, league: int, season: int, team: int, date: str = None):
   
        params = {
        "league": league,
        "season": season,
        "team": team
        }
        if date:
            params["date"] = date
    
        data = self.api_call("teams/statistics", params)
    
        # Returns a single dict with all team stats
        response = data.get("response", {})
    
        if not response:
            return {}
    
        return {
        "team_id": response.get("team", {}).get("id"),
        "team_name": response.get("team", {}).get("name"),
        "team_logo": response.get("team", {}).get("logo"),
        "league_id": response.get("league", {}).get("id"),
        "league_name": response.get("league", {}).get("name"),
        "season": response.get("league", {}).get("season"),
        "form": response.get("form"),
        
        # Fixtures
        "fixtures_played_home": response.get("fixtures", {}).get("played", {}).get("home"),
        "fixtures_played_away": response.get("fixtures", {}).get("played", {}).get("away"),
        "fixtures_played_total": response.get("fixtures", {}).get("played", {}).get("total"),
        "fixtures_wins_home": response.get("fixtures", {}).get("wins", {}).get("home"),
        "fixtures_wins_away": response.get("fixtures", {}).get("wins", {}).get("away"),
        "fixtures_wins_total": response.get("fixtures", {}).get("wins", {}).get("total"),
        "fixtures_draws_home": response.get("fixtures", {}).get("draws", {}).get("home"),
        "fixtures_draws_away": response.get("fixtures", {}).get("draws", {}).get("away"),
        "fixtures_draws_total": response.get("fixtures", {}).get("draws", {}).get("total"),
        "fixtures_loses_home": response.get("fixtures", {}).get("loses", {}).get("home"),
        "fixtures_loses_away": response.get("fixtures", {}).get("loses", {}).get("away"),
        "fixtures_loses_total": response.get("fixtures", {}).get("loses", {}).get("total"),
        
        # Goals
        "goals_for_total_home": response.get("goals", {}).get("for", {}).get("total", {}).get("home"),
        "goals_for_total_away": response.get("goals", {}).get("for", {}).get("total", {}).get("away"),
        "goals_for_total": response.get("goals", {}).get("for", {}).get("total", {}).get("total"),
        "goals_for_avg_home": response.get("goals", {}).get("for", {}).get("average", {}).get("home"),
        "goals_for_avg_away": response.get("goals", {}).get("for", {}).get("average", {}).get("away"),
        "goals_for_avg_total": response.get("goals", {}).get("for", {}).get("average", {}).get("total"),
        "goals_against_total_home": response.get("goals", {}).get("against", {}).get("total", {}).get("home"),
        "goals_against_total_away": response.get("goals", {}).get("against", {}).get("total", {}).get("away"),
        "goals_against_total": response.get("goals", {}).get("against", {}).get("total", {}).get("total"),
        "goals_against_avg_home": response.get("goals", {}).get("against", {}).get("average", {}).get("home"),
        "goals_against_avg_away": response.get("goals", {}).get("against", {}).get("average", {}).get("away"),
        "goals_against_avg_total": response.get("goals", {}).get("against", {}).get("average", {}).get("total"),
        
        # "Biggest Stats lol"
        "biggest_streak_wins": response.get("biggest", {}).get("streak", {}).get("wins"),
        "biggest_streak_draws": response.get("biggest", {}).get("streak", {}).get("draws"),
        "biggest_streak_loses": response.get("biggest", {}).get("streak", {}).get("loses"),
        "biggest_wins_home": response.get("biggest", {}).get("wins", {}).get("home"),
        "biggest_wins_away": response.get("biggest", {}).get("wins", {}).get("away"),
        "biggest_loses_home": response.get("biggest", {}).get("loses", {}).get("home"),
        "biggest_loses_away": response.get("biggest", {}).get("loses", {}).get("away"),
        
        # Clean sheets
        "clean_sheet_home": response.get("clean_sheet", {}).get("home"),
        "clean_sheet_away": response.get("clean_sheet", {}).get("away"),
        "clean_sheet_total": response.get("clean_sheet", {}).get("total"),
        
        # Failed to score, very interesting stat imo
        "failed_to_score_home": response.get("failed_to_score", {}).get("home"),
        "failed_to_score_away": response.get("failed_to_score", {}).get("away"),
        "failed_to_score_total": response.get("failed_to_score", {}).get("total"),
        
        # Penalty
        "penalty_scored_total": response.get("penalty", {}).get("scored", {}).get("total"),
        "penalty_scored_percentage": response.get("penalty", {}).get("scored", {}).get("percentage"),
        "penalty_missed_total": response.get("penalty", {}).get("missed", {}).get("total"),
        "penalty_missed_percentage": response.get("penalty", {}).get("missed", {}).get("percentage"),
        
        # Cards - extract totals from time intervals
        "yellow_cards_total": sum([
            response.get("cards", {}).get("yellow", {}).get(interval, {}).get("total", 0) or 0
            for interval in ["0-15", "16-30", "31-45", "46-60", "61-75", "76-90", "91-105", "106-120"]
        ]),
        "red_cards_total": sum([
            response.get("cards", {}).get("red", {}).get(interval, {}).get("total", 0) or 0
            for interval in ["0-15", "16-30", "31-45", "46-60", "61-75", "76-90", "91-105", "106-120"]
        ]),
        
        # Lineups - most used formation
        "most_used_formation": response.get("lineups", [{}])[0].get("formation") if response.get("lineups") else None,
        "formation_played": response.get("lineups", [{}])[0].get("played") if response.get("lineups") else None
    }


    #OG H2H API call is not in our version, so need it make manual
    def get_h2h(self, team1_id: int, team2_id: int, league: int = None, 
           season: int = None, last: int = None, status: str = None):

        print(f"Fetching H2H between team {team1_id} and team {team2_id}...")
    
        # Get all fixtures for team1
        params = {"team": team1_id}
        if league is not None: params["league"] = league
        if season is not None: params["season"] = season
        if last is not None: params["last"] = last
        if status: params["status"] = status
    
        data = self.api_call("fixtures", params)
    
        # Filter for matches against team2
        h2h_matches = []
        for item in data.get("response", []):
            home_id = item.get("teams", {}).get("home", {}).get("id")
            away_id = item.get("teams", {}).get("away", {}).get("id")
        
        # Check if team2 is involved in this match
        if home_id == team2_id or away_id == team2_id:
            h2h_matches.append({
                "fixture_id": item.get("fixture", {}).get("id"),
                "referee": item.get("fixture", {}).get("referee"),
                "date": item.get("fixture", {}).get("date"),
                "timestamp": item.get("fixture", {}).get("timestamp"),
                "venue_name": item.get("fixture", {}).get("venue", {}).get("name"),
                "venue_city": item.get("fixture", {}).get("venue", {}).get("city"),
                "status": item.get("fixture", {}).get("status", {}).get("long"),
                "league_name": item.get("league", {}).get("name"),
                "league_round": item.get("league", {}).get("round"),
                "home_team_id": home_id,
                "home_team_name": item.get("teams", {}).get("home", {}).get("name"),
                "home_team_winner": item.get("teams", {}).get("home", {}).get("winner"),
                "away_team_id": away_id,
                "away_team_name": item.get("teams", {}).get("away", {}).get("name"),
                "away_team_winner": item.get("teams", {}).get("away", {}).get("winner"),
                "home_goals": item.get("goals", {}).get("home"),
                "away_goals": item.get("goals", {}).get("away"),
                "winner": (item.get("teams", {}).get("home", {}).get("name") 
                          if item.get("teams", {}).get("home", {}).get("winner") 
                          else (item.get("teams", {}).get("away", {}).get("name") 
                               if item.get("teams", {}).get("away", {}).get("winner") 
                               else "Draw"))
            })
    
        return pd.DataFrame(h2h_matches)