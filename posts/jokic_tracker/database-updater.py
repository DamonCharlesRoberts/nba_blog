# title: database updater for Jokic triple double tracker

# importing some helpful dataframe and sql libraries
import pandas as pd
import duckdb as db
import plotly.express as px
import plotly.io as pio
#import plotly.graph_objects as go
# importing the nba_api
from nba_api.stats.static import players
from nba_api.stats.endpoints import playercareerstats
from nba_api.stats.endpoints import playergamelog
# setting up in-memory duckdb connection
conn = db.connect('../../data/triple-double-tracker.db')

# Get Jokic player ID
jokic_id = players.find_players_by_last_name('Jokic')[0]['id']

# Get Jokic seasons
seasons_df = playercareerstats.PlayerCareerStats(
    player_id=jokic_id
).get_data_frames()[0]
seasons_list = seasons_df['SEASON_ID'].tolist()

# Jokic stats
stats_list = [playergamelog.PlayerGameLog(player_id=jokic_id, season=x).get_data_frames()[0] for x in seasons_list]
jokic_stats_df = pd.concat(stats_list)

# Update the database table
conn.execute(
    '''
    CREATE OR REPLACE TABLE main AS SELECT * FROM jokic_stats_df
    '''
)

# close the connection
conn.close()