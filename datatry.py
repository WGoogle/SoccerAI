import time
import requests
from typing import Optional, Dict
import os

class ComprehensiveSoccerDataIngestion:
    
    def __init__(self, api_key: Optional[str] = None, rate_limit_delay: float = 1.0): 
        self.api_key = api_key
        self.base_url = "https://v3.football.api-sports.io"
        self.headers = {
            "x-apisports-key": api_key,
        }
        self.cache_dir = "soccer_data_cache"
        self.rate_limit_delay = rate_limit_delay  # Worried about being on free plan for API
        os.makedirs(self.cache_dir, exist_ok=True)
        
        # Ensuring I got API connection
        self._test_connection()
    
    def _test_connection(self):
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
        
    def _api_call(self, endpoint: str, params: Dict, retries: int = 3) -> Dict:
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
                    print(f" Rate limit gotten so waiting {wait_time}s before retry...")
                    time.sleep(wait_time)
                    continue
                
                response.raise_for_status() #good saftey check
                
                # By this point, the call was successful, so now delay the next call for rate limit prevention
                time.sleep(self.rate_limit_delay)

                remaining = response.headers.get('x-ratelimit-requests-remaining')
                limit = response.headers.get('x-ratelimit-requests-limit')
                print(f"API Quota: {remaining}/{limit} remaining today.") # A visual representation of API Calls left too
 
                
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
    
    def get_player_stats(self, player_id: int, season: int) -> Dict:
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
        
        return self._api_call("players", params)