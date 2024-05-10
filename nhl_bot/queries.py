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

def get_upcoming_games_query(user_id):
    return f"""
        SELECT
            g.game_source_id,
            g.eastern_start_time::timestamp at time zone 'UTC+3' as moscow_time,
            g.home_team_name,
            g.visiting_team_name,
            CASE WHEN b.user_id IS NULL THEN false ELSE true END as bet_placed
        FROM public.games_wide_datamart g
        LEFT JOIN public.users_bets b ON g.game_source_id = b.game_id AND b.user_id = {user_id}
        WHERE g.eastern_start_time::timestamp at time zone 'UTC+3' BETWEEN now() AND now() + interval '2 days'
        AND g.home_score = 0 AND g.visiting_score = 0
        ORDER BY g.eastern_start_time;

    """

def save_user_bet(user_id, game_id, home_team_winner, home_score=0, visiting_score=0):
    return f"""
        INSERT INTO public.users_bets (
            user_id, 
            game_id, 
            home_team_winner, 
            home_score, 
            visiting_score, 
            selected_at
        ) VALUES (
            {user_id}, 
            {game_id}, 
            {home_team_winner}, 
            {home_score}, 
            {visiting_score}, 
            NOW()
        )
        ON CONFLICT (user_id, game_id) DO UPDATE SET
            home_team_winner = EXCLUDED.home_team_winner,
            home_score = EXCLUDED.home_score,
            visiting_score = EXCLUDED.visiting_score,
            selected_at = NOW();
    """

def get_game_details_query(game_id):
    return f"""
        SELECT game_source_id, eastern_start_time::timestamp at time zone 'UTC+3' as moscow_time, home_team_name, visiting_team_name
        FROM public.games_wide_datamart
        WHERE game_source_id = {game_id}
    """

def get_upcoming_preds_query():
    return f"""
        WITH prep as (
        SELECT 
            game_source_id, 
            eastern_start_time::timestamp at time zone 'UTC+3' as moscow_time,
            home_team_name, 
            visiting_team_name, 
            home_team_win,
            home_team_win_proba,
            case when home_team_win = 1 then 10 * round(home_team_win_proba::numeric, 1) else 10 * round(1 - home_team_win_proba::numeric, 1) end as confidence_level,
            row_number() over (partition by home_team_name, visiting_team_name order by eastern_start_time) as rn
        FROM public.games_winner_prediction
        WHERE True
            AND eastern_start_time::timestamp at time zone 'UTC+3' BETWEEN now() AND now() + interval '2 days'
        )
        SELECT
            game_source_id, 
            moscow_time,
            home_team_name, 
            visiting_team_name, 
            home_team_win,
            confidence_level
        FROM prep
        WHERE rn = 1
        ORDER BY home_team_win_proba DESC
    """
