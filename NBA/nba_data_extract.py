from nba_api.live.nba.endpoints import scoreboard, boxscore
import psycopg2
import os
from datetime import datetime
from psycopg2.extras import execute_values

# connecting to database
DB_PARAMS = {
    "dbname": os.environ.get("DB_NAME"),
    "user": os.environ.get("DB_USER"),
    "password": os.environ.get("DB_PASSWORD"),
    "host": os.environ.get("DB_HOST"),
    "port": os.environ.get("DB_PORT"),
}

def get_db_connection():
    return psycopg2.connect(**DB_PARAMS)

# getting all live games
def fetch_all_live_games():
    try:
        # scoreboard
        live_games_data = scoreboard.ScoreBoard().get_dict()
        games = live_games_data.get("scoreboard", {}).get("games", [])

        if not games:
            print("No live games available right now.")
            return []

        print(f"Fetching data for {len(games)} games...\n")
        extracted_games = []

        # going through all live games
        for game in games:
            game_id = game.get("gameId")
            game_status = game.get("gameEt")
            home_team = game.get("homeTeam", {}).get("teamName")
            home_team_id = game.get("homeTeam", {}).get("teamId")
            away_team = game.get("awayTeam", {}).get("teamName")
            away_team_id = game.get("awayTeam", {}).get("teamId")

            print(f"Fetching live data for {home_team} (ID: {home_team_id}) vs. {away_team} (ID: {away_team_id}) ({game_status}):")

            # box score
            try:
                live_data = boxscore.BoxScore(game_id).get_dict()
                game_info = live_data.get("game", {})

                extracted_data = {
                    "gameId": game_id,
                    "gameStatusText": game_info.get("gameStatusText"),
                    "arenaName": game_info.get("arena", {}).get("arenaName"),
                    "homeTeam": extract_team_data(game_info.get("homeTeam", {})),
                    "awayTeam": extract_team_data(game_info.get("awayTeam", {})),
                }

                # Print game summary in terminal ( not req )
                print(f"Game ID: {extracted_data['gameId']}")
                print(f"Game Status: {extracted_data['gameStatusText']}")
                print(f"Arena: {extracted_data['arenaName']}\n")


                extracted_games.append(extracted_data)

                # data load in database
                insert_game_data(extracted_data)

            except Exception as e:
                print(f"Error fetching data for game ID {game_id}: {e}")

        return extracted_games

    except Exception as e:
        print(f"Error fetching live games data: {e}")
        return []

def extract_team_data(team):
    return {
        "teamId": team.get("teamId"),
        "teamName": team.get("teamName"),
        "score": team.get("score"),
        "quarters": {
            1: team.get("statistics", {}).get("quarter1"),
            2: team.get("statistics", {}).get("quarter2"),
            3: team.get("statistics", {}).get("quarter3"),
            4: team.get("statistics", {}).get("quarter4"),
        },
        "freeThrowsPercentage": team.get("statistics", {}).get("freeThrowsPercentage"),
        "freeThrowsMade": team.get("statistics", {}).get("freeThrowsMade"),
        "freeThrowsAttempted": team.get("statistics", {}).get("freeThrowsAttempted"),
        "threePointersPercentage": team.get("statistics", {}).get("threePointersPercentage"),
        "threePointersMade": team.get("statistics", {}).get("threePointersMade"),
        "threePointersAttempted": team.get("statistics", {}).get("threePointersAttempted"),
        "twoPointersPercentage": team.get("statistics", {}).get("twoPointersPercentage"),
        "twoPointersMade": team.get("statistics", {}).get("twoPointersMade"),
        "twoPointersAttempted": team.get("statistics", {}).get("twoPointersAttempted"),
        "fieldGoalsPercentage": team.get("statistics", {}).get("fieldGoalsPercentage"),
        "fieldGoalsMade": team.get("statistics", {}).get("fieldGoalsMade"),
        "fieldGoalsAttempted": team.get("statistics", {}).get("fieldGoalsAttempted"),
        "reboundsTotal": team.get("statistics", {}).get("reboundsTotal"),
        "reboundsDefensive": team.get("statistics", {}).get("reboundsDefensive"),
        "reboundsOffensive": team.get("statistics", {}).get("reboundsOffensive"),
        "foulsTechnical": team.get("statistics", {}).get("foulsTechnical"),
        "foulsPersonal": team.get("statistics", {}).get("foulsPersonal"),
        "turnovers": team.get("statistics", {}).get("turnovers"),
        "steals": team.get("statistics", {}).get("steals"),
        "blocks": team.get("statistics", {}).get("blocks"),
        "assists": team.get("statistics", {}).get("assists"),
    }

def insert_game_data(game_data):
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            
            game_metadata_query = """
            INSERT INTO nbagames (game_id, home_team_id, away_team_id, game_status, game_date) 
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (game_id) DO NOTHING;
            """
            game_id = game_data["gameId"]
            home_team_id = game_data["homeTeam"]["teamId"]
            away_team_id = game_data["awayTeam"]["teamId"]
            game_status = game_data["gameStatusText"]
            try:
               game_date = datetime.strptime(game_data["gameStatusText"], "%Y-%m-%dT%H:%M:%SZ")
            except ValueError:
               game_date = None 

            cur.execute(game_metadata_query, (game_id, home_team_id, away_team_id, game_status, game_date if game_date else None))

            # loasding team into nbateams table 
            for team in [game_data["homeTeam"], game_data["awayTeam"]]:
                team_query = """
                INSERT INTO nbateams (team_id, team_name)
                VALUES (%s, %s)
                ON CONFLICT (team_id) DO NOTHING;
                """
                team_id = team["teamId"]
                team_name = team["teamName"]
                cur.execute(team_query, (team_id, team_name))

            # loading stats in nbagame_stats table
            for team in [game_data["homeTeam"], game_data["awayTeam"]]:
                for quarter, score in team["quarters"].items():
                    stats_query = """
                    INSERT INTO nbagame_stats (
                        game_id, team_id, quarter, 
                        free_throws_percentage, free_throws_made, free_throws_attempted, 
                        three_pointers_percentage, three_pointers_made, three_pointers_attempted, 
                        two_pointers_percentage, two_pointers_made, two_pointers_attempted, 
                        field_goals_percentage, field_goals_made, field_goals_attempted, 
                        rebounds_total, rebounds_defensive, rebounds_offensive, 
                        fouls_technical, fouls_personal, turnovers, steals, blocks, assists, score
                    ) 
                    VALUES (
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                    ) 
                    ON CONFLICT (game_id, team_id, quarter) 
                    DO UPDATE SET 
                        free_throws_percentage = EXCLUDED.free_throws_percentage,
                        free_throws_made = EXCLUDED.free_throws_made,
                        free_throws_attempted = EXCLUDED.free_throws_attempted,
                        three_pointers_percentage = EXCLUDED.three_pointers_percentage,
                        three_pointers_made = EXCLUDED.three_pointers_made,
                        three_pointers_attempted = EXCLUDED.three_pointers_attempted,
                        two_pointers_percentage = EXCLUDED.two_pointers_percentage,
                        two_pointers_made = EXCLUDED.two_pointers_made,
                        two_pointers_attempted = EXCLUDED.two_pointers_attempted,
                        field_goals_percentage = EXCLUDED.field_goals_percentage,
                        field_goals_made = EXCLUDED.field_goals_made,
                        field_goals_attempted = EXCLUDED.field_goals_attempted,
                        rebounds_total = EXCLUDED.rebounds_total,
                        rebounds_defensive = EXCLUDED.rebounds_defensive,
                        rebounds_offensive = EXCLUDED.rebounds_offensive,
                        fouls_technical = EXCLUDED.fouls_technical,
                        fouls_personal = EXCLUDED.fouls_personal,
                        turnovers = EXCLUDED.turnovers,
                        steals = EXCLUDED.steals,
                        blocks = EXCLUDED.blocks,
                        assists = EXCLUDED.assists,
                        score = EXCLUDED.score;
                    """
                    cur.execute(stats_query, (
                        game_data["gameId"], team["teamId"], quarter,
                        team["freeThrowsPercentage"], team["freeThrowsMade"], team["freeThrowsAttempted"],
                        team["threePointersPercentage"], team["threePointersMade"], team["threePointersAttempted"],
                        team["twoPointersPercentage"], team["twoPointersMade"], team["twoPointersAttempted"],
                        team["fieldGoalsPercentage"], team["fieldGoalsMade"], team["fieldGoalsAttempted"],
                        team["reboundsTotal"], team["reboundsDefensive"], team["reboundsOffensive"],
                        team["foulsTechnical"], team["foulsPersonal"], team["turnovers"], team["steals"], team["blocks"], team["assists"], team["score"]
                    ))

            conn.commit()

    except Exception as e:
        print(f"Error inserting data into database: {e}")
    finally:
        conn.close()

# Run the script
fetch_all_live_games()
