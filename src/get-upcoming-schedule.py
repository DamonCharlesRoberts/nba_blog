from nba_api.stats.static import teams
from itertools import islice
import requests
import pandas as pd

nuggets_id = teams.find_team_by_abbreviation('DEN')['id']

url = 'https://cdn.nba.com/static/json/staticData/scheduleLeagueV2.json'
json = requests.get(url).json().get('leagueSchedule').get('gameDates')
df = pd.DataFrame.from_dict(json)

list(islice(json, 0, 2))