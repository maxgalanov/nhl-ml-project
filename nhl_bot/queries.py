# Запросы к БД, которые используются в боте

def get_skaters_stats_query(player_name):
    return f"""
        SELECT 
	    headshot, player_full_name, team_logo, birth_date, date_part('year', age(birth_date::date)) as years_old, birth_city, birth_country, position_code, team_business_id, team_full_name, 
        SUM(game_cnt) as game_cnt, SUM(goals) as goals, SUM(points) as points, SUM(plus_minus) as plus_minus
        FROM public.skaters_agg
        WHERE player_full_name = '{player_name}'
            AND season = (SELECT max(season) FROM public.skaters_agg)
        GROUP BY headshot, player_full_name, team_logo, birth_date, years_old, birth_city, birth_country, position_code, team_business_id, team_full_name
    """

def get_goalies_stats_query(player_name):
    return f"""
        SELECT 
            headshot, player_full_name, team_logo, birth_date, date_part('year', age(birth_date::date)) as years_old, birth_city, birth_country, position_code, team_business_id, team_full_name, 
            SUM(game_cnt) as game_cnt, (SUM(shots_against) - SUM(goals_against)) / SUM(shots_against) as shots_against_pctg
        FROM public.goalies_agg
        WHERE player_full_name = '{player_name}'
            AND season = (SELECT max(season) FROM public.goalies_agg)
        GROUP BY headshot, player_full_name, team_logo, birth_date, years_old, birth_city, birth_country, position_code, team_business_id, team_full_name
    """

def get_team_stats_query(team_name):
    return f"""
        SELECT 
            team_name, team_business_id, conference_name, division_name, games_played, league_sequence, points, win_pctg, wins, goal_for, goal_against
        FROM public.teams_stat_agg
        WHERE True
            AND (team_name = '{team_name}' or team_business_id = '{team_name}')
            AND season_id = (SELECT max(season_id) FROM public.teams_stat_agg)
    """

def get_team_logo_query(team_name):
    return f"""
        SELECT team_logo 
        FROM dwh_detailed.sat_teams_core
        WHERE true
            AND team_name = '{team_name}'
            AND is_active = 'True'
    """

def get_teams_query():
    return f"""
        SELECT 
        distinct team_name
        FROM public.teams_stat_agg
        WHERE True
            AND season_id = (SELECT max(season_id) FROM public.teams_stat_agg)
    """