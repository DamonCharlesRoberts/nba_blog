# title: database updater for Jokic triple double tracker and predictor

# importing some helpful dataframe and sql libraries
import pandas as pd
import duckdb as db
# importing the nba_api
from nba_api.stats.static import players, teams
from nba_api.stats.endpoints import playercareerstats, playergamelog, leaguegamefinder

# setting up in-memory duckdb connection
conn = db.connect('./data/triple-double-tracker.db')

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
    CREATE OR REPLACE TABLE main 
    AS SELECT 
        RIGHT(SEASON_ID, 4) AS Season,
        Player_ID AS PlayerID,
        Game_ID AS GameID,
        (
            CASE
                WHEN 
                    CAST(SPLIT_PART(strftime(strptime(GAME_DATE, '%b %d, %Y'), '%m/%d'), '/', 1) AS int) < 10 
                THEN
                    CAST(SPLIT_PART(strftime(strptime(GAME_DATE, '%b %d, %Y'), '%m/%d'), '/', 1) AS int) + 12
                ELSE
                    CAST(SPLIT_PART(strftime(strptime(GAME_DATE, '%b %d, %Y'), '%m/%d'), '/', 1) AS int)
            END
        ) AS GameMonth,
        (
            SPLIT_PART(strftime(strptime(GAME_DATE, '%b %d, %Y'), '%m/%d'), '/', 2)
        ) AS GameDay,
        (
            CASE
                WHEN MATCHUP LIKE '%@%' THEN 0
                WHEN MATCHUP LIKE '%vs.%' THEN 1
            END
        ) AS Location,
        (
            RIGHT(MATCHUP, 3)
        ) AS Opponent,
        WL,
        MIN AS Min,
        PTS AS Pts,
        AST AS Ast,
        REB AS Reb,
        (
            CASE
                WHEN PTS >= 10 AND AST >= 10 AND REB >= 10 THEN 1
                ELSE 0
            END
        ) AS TripleDouble

    FROM 
        jokic_stats_df
    '''
)

# close the connection
conn.close()

# Print
print("Database updated")