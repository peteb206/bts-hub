from flask import Flask, jsonify, render_template, request
import pandas as pd
import numpy as np
import requests
import json
import os
import sys
import math
import datetime
import time
from dateutil import tz
import pymongo
from bs4 import BeautifulSoup
import re
import statsmodels.api as sm


app = Flask(__name__)
global session, header
session = requests.Session()
header = {
  "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/88.0.4324.182 Safari/537.36",
  "X-Requested-With": "XMLHttpRequest"
}
requests.packages.urllib3.disable_warnings(requests.packages.urllib3.exceptions.InsecureRequestWarning)


@app.route('/home')
def home():
    return render_template('index.html')


@app.route('/loadTableData')
def index():
    start_time = time.time() # Start timer

    date_arg = request.args.get('date')
    min_hits = int(request.args.get('hitMin'))
    is_today = bool(request.args.get('isToday'))
    from_app = bool(request.args.get('fromApp'))

    # This year
    if from_app == True:
        date_arg = date_arg.split(',')[0] # Only use the first date when reading from the app
    dates = date_arg.split(',')
    print('\n------------------------------', 'Calculating predictions for {}...'.format(dates), sep='\n')
    first_date_year = datetime.datetime.strptime(dates[0], '%Y-%m-%d').year
    this_years_games_df = get_schedule(year=first_date_year, lineups=False)
    player_info_df = get_player_info(year=first_date_year, hitters=True, pitchers=True)
    statcast_year_df = get_statcast_events(this_years_games_df) # Query this year's data from database
    statcast_year_df['hit'] = statcast_year_df['events'].apply(lambda event: is_hit(event))
    player_hits_by_game_df = statcast_year_df.groupby(['game_pk', 'batter'])['hit'].sum().reset_index()
    statcast_year_df = enrich_data(statcast_year_df, this_years_games_df, player_info_df)

    all_dfs, opponent_starter_df, opponent_bullpen_df, todays_games_df, start_date, end_date = list(), None, None, None, None, None # Initialize some variables before iterating
    for date_string in dates:
        # Today
        today = datetime.datetime.strptime(date_string, '%Y-%m-%d')
        todays_games_df = get_schedule(year=today.year, date=today, lineups=True, is_today=is_today)
        print('--- NOTE: There are {} games on {}...'.format(len(todays_games_df.index), date_string))
        if len(todays_games_df.index) > 0:
            # There are games today
            statcast_df = statcast_year_df[statcast_year_df['game_date'] < date_string]
            starting_pitcher_df = todays_games_df.melt(id_vars=['away_starter_id', 'home_starter_id'], value_vars=['away_team', 'home_team'], var_name='home_away', value_name='team')
            starting_pitcher_df['pitcher'] = np.where(starting_pitcher_df['home_away'] == 'away_team', starting_pitcher_df['away_starter_id'], starting_pitcher_df['home_starter_id'])
            starting_pitcher_teams = starting_pitcher_df.set_index('pitcher')['team'].to_dict()
            todays_starting_pitchers = list(starting_pitcher_teams.keys())

            game_date_values = statcast_df['game_date'].values
            start_date, end_date = game_date_values[0], game_date_values[-1]

            calculations_df = pd.merge(calculate_hit_pct(statcast_df, weighted = False), calculate_hit_pct(statcast_df, weighted = True), on='batter', suffixes=('_total', '_weighted'))

            calculate_per_pa_dfs = calculate_hit_per_pa(statcast_df)
            calculations_df = pd.merge(calculate_per_pa_dfs[0], calculations_df, how='right', on='batter').rename({'L': 'H_per_PA_vs_L', 'R': 'H_per_PA_vs_R'}, axis=1)
            calculations_df = pd.merge(calculate_per_pa_dfs[1], calculations_df, how='right', on='batter').rename({'H_per_PA': 'H_per_PA_vs_BP'}, axis=1)

            opponent_starter_df = statcast_df[statcast_df['pitcher'].isin(todays_starting_pitchers)].groupby('pitcher')[['hit', 'xBA']].mean().round(3).reset_index()
            opponent_starter_df = pd.merge(opponent_starter_df, calculate_per_pa_dfs[2], how='left', on='pitcher')
            opponent_starter_df = pd.merge(opponent_starter_df, player_info_df, left_on='pitcher', right_on='id').drop(['id', 'team', 'B'], axis=1).set_index('pitcher')
            opponent_bullpen_df = statcast_df[statcast_df['starter_flg'] == False].groupby('pitching_team')[['hit', 'xBA']].mean()

            healthy_players_df = player_info_df[~player_info_df['id'].isin(injured_player_ids(year=today.year))] if is_today == True else player_info_df
            all_df = get_hit_probability(calculations_df, healthy_players_df, todays_games_df, opponent_starter_df, opponent_bullpen_df)
            all_df = pd.merge(all_df, player_hits_by_game_df, how='left', on=['game_pk', 'batter']) # If not today, include how many hits player actually got in game
            if len(dates) > 1:
                all_df['date'] = date_string
            all_dfs.append(all_df)

    all_df = pd.concat(all_dfs, ignore_index=True)

    if from_app == False:
        out = jsonify(all_df[all_df['order'] > -1].drop(['name', 'team', 'B'], axis=1).fillna(0).round(4).to_dict(orient='records'))
    else:
        all_df = color_columns(all_df, min_hits)
        out = jsonify({
            'rows': all_df[['game_pk', 'batter', 'probability', 'hit']].fillna(0).round(4).drop_duplicates().sort_values(by='probability', ascending=False).to_dict(orient='records'),
            'metrics': all_df.drop(['game_pk', 'probability', 'hit', 'order'], axis=1).drop_duplicates(subset='batter').set_index('batter').round(3).to_dict('index'),
            'opponents': {
                'starters': opponent_starter_df.fillna(0).round(3).to_dict(orient='index'),
                'bullpens': opponent_bullpen_df.fillna(0).round(3).to_dict(orient='index')
            },
            'games': todays_games_df.set_index('game_pk').to_dict(orient='index'),
            'headToHead': {},
            'weather': get_weather() if is_today else {},
            'startDate': start_date,
            'endDate': end_date
        })

    stop_timer('\nTotal', start_time) # Stop timer
    return out


def get_statcast_events(this_years_games_df):
    start_time = time.time() # Start timer
    print('--- NOTE: retrieving statcast events from database...')

    collection = read_database()
    # Query the year's data from database
    entries = list(collection.find(
        {
            'game_pk': {
                '$in': this_years_games_df['game_pk'].tolist()
            }
        }, {
            '_id': False
        }
    ))

    out = pd.DataFrame.from_records(entries)

    stop_timer('get_statcast_events(): {} events... '.format(len(entries)), start_time) # Stop timer
    return out


@app.route('/pickHistory')
def get_pick_history():
    start_time = time.time() # Start timer

    accounts = ['peteb206', 'pberryman6']

    picks = list()
    for account in accounts:
        okta_uid = os.environ.get('OKTA_UID_{}'.format(account.upper()))
        url = f'https://fantasy-lookup-service.mlb.com/fantasylookup/json/named.bts_profile_cmpsd.bam?bts_game_id=12&bts_user_recent_results.maxRows=200&timeframe=365&fntsy_game_id=10&bts_mulligan_status.game_id=bts2021&okta_uid={okta_uid}'
        full_json = json.loads(session.get(url, headers = header, timeout = 10).text)
        pick_history = full_json['bts_profile_cmpsd']['bts_user_recent_results']['queryResults']['row']
        pick_history_df = pd.DataFrame(pick_history)
        pick_history_df['game_date'] = pick_history_df['game_date'].apply(lambda x: x.split('T')[0])
        pick_history_df['account'] = account
        pick_history_df.rename({'name_display_first_last': 'name'}, axis = 1, inplace = True)
        picks += pick_history_df.to_dict('records')

    out = jsonify({'data': picks})
    stop_timer('get_pick_history', start_time) # Stop timer
    return out


@app.route('/scrapeStatcast')
def get_statcast_data():
    start_time = time.time() # Start timer

    year = int(request.args.get('year'))

    today = datetime.date(year, 12, 31)
    start_date = datetime.date(year, 1, 1)
    savant_scrape_days_span = 9
    savant_scrape_date_offset_minus_1, savant_scrape_date_offset = datetime.timedelta(days=savant_scrape_days_span - 1), datetime.timedelta(days=savant_scrape_days_span)

    df_list = list()
    collection = read_database()
    year_schedule_df = get_schedule(year=year)
    # Retrive database entries from the specified season
    existing_data_df = pd.DataFrame(list(collection.find({'game_pk': {"$in": year_schedule_df['game_pk'].tolist()}})))

    last_date = start_date.strftime('%Y-%m-%d')
    if len(existing_data_df.index) > 0:
        df_list.append(existing_data_df)
        last_game = existing_data_df['game_pk'].values[-1]
        last_date = get_schedule(game_pk=last_game)['game_date'].values[-1]
        print(f'The database has data up to {last_date}', '\n', sep='')
        start_date = datetime.datetime.strptime(last_date, '%Y-%m-%d').date() + datetime.timedelta(days=1)

    while start_date <= today:
        start_date_str, end_date_str = start_date.strftime('%Y-%m-%d'), (start_date + savant_scrape_date_offset_minus_1).strftime('%Y-%m-%d')
        url = 'https://baseballsavant.mlb.com/statcast_search/csv?'
        url_params = [
            'all=true',
            'hfGT=R%7C',
            'hfSea={}%7C'.format(start_date.year),
            'player_type=batter',
            'game_date_gt={}'.format(start_date_str),
            'game_date_lt={}'.format(end_date_str),
            'min_pitches=0',
            'min_results=0',
            'group_by=name',
            'sort_col=pitches',
            'player_event_sort=api_p_release_speed',
            'sort_order=desc',
            'min_pas=0',
            'type=details'
        ]
        url += '&'.join(url_params)

        df = pd.read_csv(url , usecols=['game_pk', 'game_date', 'inning_topbot', 'at_bat_number', 'batter', 'pitcher', 'events', 'estimated_ba_using_speedangle'])
        obs = len(df.index)
        interval = ' to '.join([start_date_str, end_date_str])
        print(interval, f'{obs} results', sep=': ')

        if obs > 0:
            # Format data
            df = df[(~df['events'].isna()) & (df['events'] != '')] # keep rows that ended at bat
            df['home'] = df['inning_topbot'].apply(lambda x: True if x == 'Bot' else False) # determine if batter is on home/away team
            df['xBA'] = df['estimated_ba_using_speedangle'].fillna(0)
            df['_id'] = df.apply(lambda row: '{}_{}'.format(row['game_pk'], str(row['at_bat_number']).zfill(3)), axis=1)
            df.sort_values(by='_id', ignore_index=True, inplace=True)
            last_date = df['game_date'].values[-1]
            df_list.append(df[['_id', 'game_pk', 'batter', 'pitcher', 'home', 'xBA', 'events']])

        start_date += savant_scrape_date_offset

    df = pd.concat(df_list, ignore_index=True)

    new_entries_df = pd.concat([existing_data_df, df], ignore_index=True).drop_duplicates(subset='_id', keep=False) # drop duplicate events
    print('Adding {} plate appearances to the database'.format(len(new_entries_df.index)), '\n')
    if len(new_entries_df.index) > 0:
        collection.insert_many(new_entries_df.to_dict('records'))

    out = jsonify({'mostRecentStatcastData': last_date})
    stop_timer('get_statcast_data()', start_time) # Stop timer
    return out


def read_database():
    start_time = time.time() # Start timer

    client = pymongo.MongoClient(os.environ.get('DATABASE_CLIENT'))
    database = client['statcast']
    collection = database['statcastEvents']

    stop_timer('read_database()', start_time) # Stop timer
    return collection


def enrich_data(df, schedule_df, player_info_df):
    start_time = time.time() # Start timer

    # Calculate batting order
    df['order'] = df.groupby(['game_pk', 'home'])['batter'].cumcount() + 1
    df['order'] = np.where(df['order'] < 10, df['order'], np.nan)

    df['statcast'] = df.groupby('game_pk')['xBA'].transform('sum') > 0

    df = pd.merge(df, schedule_df.drop_duplicates(subset='game_pk', keep='last'), on='game_pk').sort_values(by='game_date')
    df['starter_flg'] = np.where(df['home'] == True, df['pitcher'] == df['home_starter_id'], df['pitcher'] == df['away_starter_id'])
    df['pitching_team'] = np.where(df['home'] == True, df['home_team'], df['away_team'])
    df = pd.merge(df, player_info_df[['id', 'B']], how='left', left_on='batter', right_on='id').drop('id', axis=1)
    df = pd.merge(df, player_info_df[['id', 'T']], how='left', left_on='pitcher', right_on='id').drop('id', axis=1).rename({'B': 'batter_handedness', 'T': 'pitcher_handedness'}, axis=1)
    df['batter_handedness'] = np.where(df['batter_handedness'] == 'S', np.where(df['pitcher_handedness'] == 'L', 'R', 'L'), df['batter_handedness'])

    stop_timer('enrich_data()', start_time) # Stop timer
    return df


def is_hit(event):
    return 1 if event in ['home_run', 'triple', 'double', 'single'] else 0


def calculate_hit_pct(statcast_df, weighted = False):
    start_time = time.time() # Start timer

    df_by_game = statcast_df.groupby(['game_date', 'game_pk', 'statcast', 'batter'])[['hit', 'xBA', 'order']].sum().reset_index().rename({'hit': 'H', 'xBA': 'xH'}, axis=1)
    df_by_game['H_1+'] = (df_by_game['H'] >= 1).astype(int)
    df_by_game['G'] = 1
    df_by_game['xH_1+'] = (df_by_game['xH'] >= 1).astype(int)
    df_by_game['statcast_G'] = np.where(df_by_game['statcast'] == True, 1, 0)

    agg_func_sum = weighted_sum if weighted == True else 'sum'
    agg_func_avg = weighted_avg if weighted == True else 'mean'
    df_by_season = df_by_game.groupby('batter').agg({'H': 'sum', 'xH': agg_func_avg, 'G': 'sum', 'H_1+': agg_func_sum, 'xH_1+': agg_func_sum, 'statcast_G': 'sum', 'order': agg_func_avg}).reset_index().rename({'xH': 'xH_per_G'}, axis=1)
    df_by_season['hit_pct'] = df_by_season['H_1+'] / df_by_season['G']
    df_by_season['x_hit_pct'] = df_by_season['xH_1+'] / df_by_season['statcast_G']
    df_by_season.drop(['H_1+', 'xH_1+', 'statcast_G'], axis=1, inplace=True)

    stop_timer('calculate_hit_pct()', start_time) # Stop timer
    return df_by_season


def calculate_hit_per_pa(statcast_df):
    start_time = time.time() # Start timer

    batter_vs_pitcher_hand_df = statcast_df.pivot_table(values='hit', index='batter', columns='pitcher_handedness').reset_index()
    batter_vs_relievers_df = statcast_df[statcast_df['starter_flg'] == False].groupby('batter')['hit'].mean().reset_index().rename({'hit': 'H_per_PA'}, axis=1)
    pitcher_vs_batter_hand_df = statcast_df.pivot_table(values='hit', index='pitcher', columns='batter_handedness').reset_index().rename({'L': 'SP_H_per_BF_vs_L', 'R': 'SP_H_per_BF_vs_R'}, axis=1)

    stop_timer('calculate_hit_per_pa()', start_time) # Stop timer
    return batter_vs_pitcher_hand_df, batter_vs_relievers_df, pitcher_vs_batter_hand_df


def get_opponent_info(statcast_df, today):
    start_time = time.time() # Start timer

    matchups = list()
    url = 'https://baseballsavant.mlb.com/schedule?date={}'.format(today.strftime('%Y-%m-%d'))
    response_json = session.get(url, headers = header).json()
    response_dict = json.loads(json.dumps(response_json))
    games = response_dict['schedule']['dates'][0]['games']
    for game in games:
        game_number = game['gameNumber']
        game_pk = game['gamePk']
        game_time_utc = datetime.datetime.strptime(game['gameDate'], '%Y-%m-%dT%H:%M:%SZ')
        game_time_utc = game_time_utc.replace(tzinfo=tz.gettz('UTC'))
        game_time_current_time_zone = game_time_utc.astimezone(tz.gettz('America/Chicago'))
        game_time_string = game_time_current_time_zone.strftime('%I:%M %p %Z')
        teams = game['teams']
        for home_away in ['away', 'home']:
            team = teams[home_away]
            team_abbreviation = team['team']['abbreviation']
            opposing_team_abbreviation = teams['home' if home_away == 'away' else 'away']['team']['abbreviation']
            matchup_dict = dict()
            matchup_dict['team'] = team_abbreviation
            matchup_dict['opponent'] = opposing_team_abbreviation
            matchup_dict['home_away'] = home_away
            matchup_dict['game_pk'] = game_pk
            matchup_dict['game_number'] = game_number
            matchup_dict['game_time'] = game_time_string if game_time_string[0] != '0' else game_time_string[1:]
            if 'probablePitcher' in team.keys():
                pitcher_id = team['probablePitcher']['id']
                matchup_dict['pitcher_id'] = pitcher_id
                matchup_dict['pitcher_name'] = team['probablePitcher']['firstLastName'] + ' (' + team['probablePitcher']['pitchHand']['code'] + ')'
                matchup_dict['sp_HA_per_BF_total'], matchup_dict['sp_xHA_per_BF_total']  = get_pitcher_stats(statcast_df[statcast_df['pitcher'] == pitcher_id])
            else:
                matchup_dict['pitcher_id'] = -1
            matchup_dict['bp_HA_per_BF_total'], matchup_dict['bp_xHA_per_BF_total'] = get_pitcher_stats(statcast_df[(statcast_df['opponent'] == team_abbreviation) & (statcast_df['starter_flg'] == False)])
            matchups.append(matchup_dict)
    matchups_df = pd.DataFrame(matchups).rename({'team': 'pitching_team'}, axis=1)

    stop_timer('get_opponent_info()', start_time) # Stop timer
    return matchups_df


def get_pitcher_stats(df, since_date=None):
    if since_date != None:
        df = df[df['game_date'] >= since_date]
    if len(df.index):
        return round(df['hit'].mean(), 2), round(df['xBA'].mean(), 2)
    else:
        return np.nan, np.nan


def color_columns(df, min_hits):
    start_time = time.time() # Start timer

    df_new = df.copy()
    df_new = df_new[df_new['H_total'] >= min_hits]
    rwg = ['#F8696B', '#F86B6D', '#F86E70', '#F87173', '#F87476', '#F87779', '#F87A7C', '#F87D7F', '#F88082', '#F88385', '#F88688', '#F8898B', '#F88C8E', '#F98F91', '#F99294', '#F99597', '#F9989A', '#F99A9D', '#F99DA0', '#F9A0A3', '#F9A3A6', '#F9A6A9', '#F9A9AC', '#F9ACAF', '#F9AFB2', '#FAB2B5', '#FAB5B7', '#FAB8BA', '#FABBBD', '#FABEC0', '#FAC1C3', '#FAC4C6', '#FAC7C9', '#FACACC', '#FACCCF', '#FACFD2', '#FAD2D5', '#FAD5D8', '#FBD8DB', '#FBDBDE', '#FBDEE1', '#FBE1E4', '#FBE4E7', '#FBE7EA', '#FBEAED', '#FBEDF0', '#FBF0F3', '#FBF3F6', '#FBF6F9', '#FBF9FC', '#FCFCFF', '#F9FBFD', '#F6FAFA', '#F3F9F8', '#F0F8F5', '#EDF6F2', '#EAF5F0', '#E7F4ED', '#E4F3EA', '#E1F1E8', '#DEF0E5', '#DBEFE2', '#D8EEE0', '#D5ECDD', '#D2EBDB', '#CFEAD8', '#CCE9D5', '#C8E7D3', '#C5E6D0', '#C2E5CD', '#BFE4CB', '#BCE2C8', '#B9E1C5', '#B6E0C3', '#B3DFC0', '#B0DDBD', '#ADDCBB', '#AADBB8', '#A7DAB6', '#A4D9B3', '#A1D7B0', '#9ED6AE', '#9BD5AB', '#98D4A8', '#94D2A6', '#91D1A3', '#8ED0A0', '#8BCF9E', '#88CD9B', '#85CC99', '#82CB96', '#7FCA93', '#7CC891', '#79C78E', '#76C68B', '#73C589', '#70C386', '#6DC283', '#6AC181', '#67C07E', '#63BE7B']
    gwr = rwg[::-1]

    percentile_columns_prefix = ['H', 'xH_per_G', 'hit_pct', 'x_hit_pct', 'sp_HA_per_BF', 'sp_xHA_per_BF', 'bp_HA_per_BF', 'bp_xHA_per_BF']
    percentile_columns_exact = ['H_vs_SP', 'xH_vs_SP', 'H_per_PA_vs_L', 'H_per_PA_vs_R', 'H_per_PA_vs_BP', 'H_per_BF_vs_L', 'H_per_BF_vs_R']
    percentile_columns_all = list()
    for prefix in percentile_columns_prefix:
        for suffix in ['total', 'weighted']:
            percentile_columns_all.append(prefix + '_' + suffix)
    percentile_columns_all += percentile_columns_exact
    descending_columns = []
    for column in percentile_columns_all:
        if (column in df_new.columns):
            df_new[column + '_color'] = df_new[column].rank(pct=True)
            df_new[column + '_color'] = df_new[column + '_color'].fillna('').apply(lambda x: (gwr[math.floor(x * 100)] if column in descending_columns else rwg[math.floor(x * 100)]) if str(x) != '' else '')

    stop_timer('color_columns()', start_time) # Stop timer
    return df_new


def get_weather():
    start_time = time.time() # Start timer

    html = session.get('https://www.rotowire.com/baseball/weather.php', headers = header).text
    soup = BeautifulSoup(html, 'lxml')

    teams = dict()
    for weather_box in soup.find_all('div', {'class': 'weather-box'}):
        weather_box_teams = weather_box.find('div', {'class': 'weather-box__teams'})
        weather_box_weather = weather_box.find('div', {'class': 'weather-box__weather'})
        for link in weather_box_teams.find_all('a'):
            teams[link['href'].split('=')[1]] = re.search('([^\/]+$)', weather_box_weather.find('img')['src']).group()
    if 'WAS' in teams.keys():
        teams['WSH'] = teams.pop('WAS')

    stop_timer('get_weather()', start_time) # Stop timer
    return teams


def get_player_info(year=2021, hitters=True, pitchers=True):
    # Ex. get_player_info(2021)
    # returns df with the following columns: id, fullName, B, T
    start_time = time.time() # Start timer

    player_df1 = pd.read_json(f'https://statsapi.mlb.com/api/v1/sports/1/players?season={year}')
    player_df2 = pd.json_normalize(player_df1['people'])
    if hitters == False:
        player_df2 = player_df2[player_df2['primaryPosition.code'] == '1']
    if pitchers == False:
        player_df2 = player_df2[player_df2['primaryPosition.code'] != '1']
    player_df3 = player_df2.rename({'fullName': 'name', 'primaryPosition.abbreviation': 'position', 'batSide.code': 'B', 'pitchHand.code': 'T'}, axis=1)[['id', 'name', 'position', 'currentTeam.id', 'B', 'T']]

    teams_url = f'https://statsapi.mlb.com/api/v1/teams?lang=en&sportId=1&season={year}'
    teams_df1 = pd.read_json(teams_url)
    teams_id_map_df = pd.json_normalize(teams_df1['teams'])[['id', 'abbreviation']].set_index('id')
    teams_id_map = teams_id_map_df['abbreviation'].to_dict()
    player_df3['team'] = player_df3['currentTeam.id'].apply(lambda teamId: teams_id_map[teamId])

    stop_timer('get_player_info()', start_time) # Stop timer
    return player_df3.drop('currentTeam.id', axis=1)


def get_schedule(year=None, date=None, game_pk=None, lineups=False, is_today=True):
    start_time = time.time() # Start timer

    col_rename_dict = {
        'gamePk': 'game_pk',
        'teams.away.team.id': 'away_id',
        'teams.home.team.id': 'home_id',
        'teams.away.probablePitcher.id': 'away_starter_id',
        'teams.home.probablePitcher.id': 'home_starter_id'
    }

    schedule_url = f'https://statsapi.mlb.com/api/v1/schedule?lang=en&sportId=1&gameType=R&'
    url_params = list()
    if year != None:
        url_params.append('season={}'.format(year))
    if date != None:
        url_params.append('date={}'.format(datetime.date.strftime(date, '%m/%d/%Y')))
    if game_pk != None:
        url_params.append('gamePk={}'.format(game_pk))
    if lineups == True:
        url_params.append('hydrate=probablePitcher,lineups,linescore')
    else:
        url_params.append('hydrate=probablePitcher')
    schedule_url += '&'.join(url_params)

    schedule_df1 = pd.read_json(schedule_url)
    if len(schedule_df1.index) == 0:
        return pd.DataFrame() # No games were scheduled for this day
    schedule_df2 = pd.json_normalize(schedule_df1['dates'])[['date', 'games']]
    schedule_df3 = schedule_df2.explode('games')
    team_matchups_df = pd.json_normalize(schedule_df3['games'])
    team_matchups_df['game_date'] = team_matchups_df['gameDate'].apply(lambda x: utc_to_central(x, 'date'))
    team_matchups_df['game_time'] = team_matchups_df.apply(lambda row: game_time_func(row, is_today=is_today), axis=1)
    if 'lineups.awayPlayers' in team_matchups_df.columns:
        col_rename_dict['lineups.awayPlayers'] = 'away_lineup'
        col_rename_dict['lineups.homePlayers'] = 'home_lineup'
    else:
        team_matchups_df['away_lineup'] = np.empty((len(team_matchups_df.index), 0)).tolist()
        team_matchups_df['home_lineup'] = np.empty((len(team_matchups_df.index), 0)).tolist()
    team_matchups_df = team_matchups_df.rename(col_rename_dict, axis=1)[['game_pk', 'game_date', 'game_time', 'away_id', 'home_id', 'away_starter_id', 'home_starter_id', 'away_lineup', 'home_lineup']]

    if year == None:
        year = schedule_df2['date'].values[-1].split('-')[0]
    teams_url = f'https://statsapi.mlb.com/api/v1/teams?lang=en&sportId=1&season={year}'
    teams_df1 = pd.read_json(teams_url)
    teams_id_map_df = pd.json_normalize(teams_df1['teams'])[['id', 'abbreviation']].set_index('id')
    teams_id_map = teams_id_map_df['abbreviation'].to_dict()
    for side in ['away', 'home']:
        team_matchups_df[f'{side}_team'] = team_matchups_df[f'{side}_id'].apply(lambda teamId: teams_id_map[teamId])
        team_matchups_df[f'{side}_starter_id'] = team_matchups_df[f'{side}_starter_id'].fillna(0).astype(int)
        team_matchups_df[f'{side}_lineup'] = team_matchups_df[f'{side}_lineup'].fillna('').apply(lambda lineup: [batter['id'] for batter in lineup])

    stop_timer('get_schedule()', start_time) # Stop timer
    return team_matchups_df.drop(['away_id', 'home_id'], axis=1)


def utc_to_central(time_string, return_type='time'):
    game_time_utc = datetime.datetime.strptime(time_string, '%Y-%m-%dT%H:%M:%SZ').replace(tzinfo=tz.gettz('UTC'))
    game_time_current_time_zone = game_time_utc.astimezone(tz.gettz('America/Chicago'))
    format = '%I:%M %p %Z' if return_type == 'time' else '%Y-%m-%d'
    return game_time_current_time_zone.strftime(format)


def lineup_func(lineups, player_id, team):
    out = 'TBD'
    if player_id in lineups.keys():
        out = lineups[player_id]
    elif team in lineups.keys():
        if lineups[team] == True:
            out = 'OUT'
    return out


def game_time_func(row, is_today):
    game_time = ''
    if row['status.statusCode'] == 'I':
        row_keys = row.keys()
        if 'linescore.currentInningOrdinal' in row_keys:
            game_time = row['linescore.currentInningOrdinal']
        if 'linescore.inningHalf' in row_keys:
            game_time = '{} {}'.format(row['linescore.inningHalf'], game_time)
    elif (is_today == False) | (row['status.statusCode'] in ['F', 'O', 'UR', 'CR']) | ('D' in row['status.statusCode']):
        game_time = row['status.detailedState']
    else:
        game_time = utc_to_central(row['gameDate'], 'time')
        if game_time[0] == '0':
            game_time = game_time[1:]
    return game_time


def weighted_avg(s):
    return np.average(s, weights = calculate_weights(s)) if len(s) > 0 else 0


def weighted_sum(s):
    s2 = s.dropna()
    out = 0
    if len(s2) > 0:
        weights = calculate_weights(s2)
        out =  np.dot(s2, weights) / np.mean(weights)
    return out


def calculate_weights(s):
    weights = list()
    for i in range(len(s)):
        weights.append(1 if i == 0 else weights[-1] * 1.1)
    return weights


def get_hit_probability(calculations_df, healthy_players_df, todays_games_df, opponent_starter_df, opponent_bullpen_df):
    start_time = time.time() # Start timer

    df = pd.merge(calculations_df, healthy_players_df[(healthy_players_df['position'] != 'P') | (healthy_players_df['name'] == 'Shohei Ohtani')], left_on='batter', right_on='id').drop(['id', 'T'], axis=1)

    todays_matchups_df = pd.DataFrame()
    for side in ['away', 'home']:
        other = 'home' if side == 'away' else 'away'
        side_df = todays_games_df.copy()
        side_df.columns = [col.replace(other, 'opponent').replace(side, 'team') for col in side_df.columns]
        side_df['home_away'] = side
        side_df = side_df.drop([col for col in ['game_date', 'game_time', 'team_starter_id', 'opponent_lineup'] if col in side_df.columns], axis=1).rename({'team_team': 'team', 'opponent_team': 'opponent', 'opponent_starter_id': 'pitcher'}, axis=1)
        todays_matchups_df = todays_matchups_df.append(side_df, ignore_index=True)
    df = pd.merge(df, todays_matchups_df, on='team')

    df = pd.merge(df, opponent_starter_df.drop(['name', 'position'], axis=1), how='left', left_on='pitcher', right_index=True, suffixes=['', '_starter'])
    df = pd.merge(df, opponent_bullpen_df, how='left', left_on='opponent', right_index=True, suffixes=['', '_bullpen'])

    df['H_per_PA_vs_SP_Hand'] = df.apply(lambda row: row['H_per_PA_vs_{}'.format(row['T'])] if type(row['T']) == type('') else (row['H_per_PA_vs_R'] + row['H_per_PA_vs_L']) / 2, axis=1)
    df['B'] = np.where(df['B'] == 'S', np.where(df['T'] == 'L', 'R', 'L'), df['B'])
    df['H_per_BF_vs_B_Hand'] = df.apply(lambda row: row['SP_H_per_BF_vs_{}'.format(row['B'])], axis=1).fillna(0)
    df['order'] = df.apply(lambda row: row['team_lineup'].index(row['batter']) / 8 if row['batter'] in row['team_lineup'] else (row['order_total'] - 1) / 8, axis=1)

    model = sm.load('log_reg_model.pickle')
    predictors = ['H_per_BF_vs_B_Hand', 'H_per_PA_vs_BP', 'H_per_PA_vs_SP_Hand', 'hit_bullpen', 'hit_pct_total', 'hit_pct_weighted', 'order', 'xBA_bullpen', 'xH_per_G_total', 'xH_per_G_weighted', 'x_hit_pct_total', 'x_hit_pct_weighted']
    df['probability'] = model.predict(df[predictors].astype(float))

    stop_timer('get_hit_probability()', start_time) # Stop timer
    # print('columns', list(df.columns), sep=': ')
    return df[['batter', 'game_pk', 'probability', 'name', 'team', 'B', 'order', 'H_per_PA_vs_BP'] + [col for col in df.columns if col.split('_')[-1] in ['total', 'weighted', 'Hand', 'bullpen']]]


@app.route('/gameLogs')
def game_logs():
    start_time = time.time() # Start timer

    player_type = request.args.get('type')
    player_id = request.args.get(player_type)
    season = request.args.get('year')

    if (player_type == None) | (player_id == None) | (season == None):
        return jsonify({'error': 'type, year and batter/pitcher are required parameters'})

    url = 'https://baseballsavant.mlb.com/statcast_search?'
    url_params = [
        'hfGT=R%7C',
        'hfSea={}%7C'.format(season),
        'player_type={}'.format(player_type),
        'batters_lookup%5B%5D={}'.format(player_id),
        'min_pitches=0',
        'min_results=0',
        'group_by=name-date',
        'player_event_sort=api_p_release_speed',
        'min_pas=0',
        'chk_stats_pa=on',
        'chk_stats_abs=on',
        'chk_stats_bip=on',
        'chk_stats_hits=on',
        'chk_stats_k_percent=on',
        'chk_stats_bb_percent=on',
        'chk_stats_babip=on',
        'chk_stats_ba=on',
        'chk_stats_xba=on'
    ]
    url += '&'.join(url_params)
    df = pd.read_html(url)[0][['Date', 'PA', 'AB', 'BIP', 'Hits', 'xBA', 'BA', 'BABIP', 'K%', 'BB%']]
    df.sort_values(by='Date', ascending=False, ignore_index=True, inplace=True)
    for pct in ['K%', 'BB%']:
        df[pct] = df[pct].apply(lambda x: x / 100 if x > 0 else 0)
    df['Date'] = df['Date'].apply(lambda x: '-'.join(x.split('-')[1:]))

    out = jsonify({'data': df.round(4).to_dict(orient='records')})
    stop_timer('game_logs()', start_time) # Stop timer
    return out


def injured_player_ids(year=None):
    start_time = time.time() # Start timer

    load_date = session.get(f'https://www.fangraphs.com/api/roster-resource/injury-report/loaddate?season={year}', headers = header, verify = False).text.strip('\"')
    injuries = json.loads(session.get(f'https://cdn.fangraphs.com/api/roster-resource/injury-report/data?loaddate={load_date}&season={year}', headers = header,).text)
    out = [injury['mlbamid'] for injury in injuries if injury['returndate'] == None]

    stop_timer('injured_player_ids()', start_time) # Stop timer
    return out


def stop_timer(function_name, start_time):
    print('{} time: {}'.format(function_name, datetime.timedelta(seconds = round(time.time() - start_time))))