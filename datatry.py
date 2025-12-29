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
                print(f"API Quota: {remaining}/{limit} remaining today.") # A visual representation of API Calls left too
 
                if remaining < 2:
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
                "league_id": item.get("leagues", {}).get("id"),
                "name": item.get("leagues", {}).get("name"),
                "team": item.get("leagues", {}).get("team"),
                "type": item.get("leagues", {}).get("type"),
                "country": item.get("leagues", {}).get("country"),
                "season": item.get("leagues", {}).get("season")
            }
            for item in data.get("response", [])
        ])