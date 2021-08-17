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


app = Flask(__name__)
global session, header
session = requests.Session()
header = {
  "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/88.0.4324.182 Safari/537.36",
  "X-Requested-With": "XMLHttpRequest"
}


@app.route('/')
def home():
    return render_template('index.html')


@app.route('/loadTableData')
def index():
    start_time = time.time() # Start timer

    day_string = request.args.get('day')
    min_hits = int(request.args.get('hitMin'))

    today = datetime.datetime.now(tz.gettz('America/Chicago')).date()
    statcast_data = get_statcast_data()
    statcast_df = statcast_data[0]
    last_date = statcast_data[1]

    general_info_df = statcast_df.drop_duplicates(subset=['batter', 'batter_handedness'], keep='last').rename({'batter': 'player_id'}, axis=1)[['player_id', 'team', 'batter_handedness']]
    general_info_df['batter_handedness'] = np.where(general_info_df['player_id'].duplicated(keep=False), 'B', general_info_df['batter_handedness'])
    general_info_df.drop_duplicates(subset='player_id', inplace=True)

    calculations_df = calculate_hit_pct(statcast_df, weighted = False)
    calculations_df_weighted = calculate_hit_pct(statcast_df, weighted = True)
    calculations_df_weighted.drop('player_name', axis=1, inplace=True)

    calculations_df = pd.merge(general_info_df, calculations_df, how='right', on='player_id')
    all_df = pd.merge(calculations_df, calculations_df_weighted, how='left', on='player_id', suffixes=('_total', '_weighted'))
    try:
        all_df = pd.merge(all_df, get_opponent_info(statcast_df, today), how='left', left_on='team', right_on='opponent').drop(['game_number', 'opponent'], axis=1).rename({'pitching_team': 'opponent'}, axis=1)
        all_df['opponent'] = np.where(all_df['home_away'] == 'away', all_df['opponent'], '@' + all_df['opponent'])
        all_df['opponent'] = all_df['opponent'] + ' (' + all_df['game_time'] + ')'
    except:
        all_df['opponent'] = ''
    all_df['player_name'] = all_df['player_name'].apply(lambda name: ' '.join(name.split(',')[::-1]).strip())

    head_to_head = batter_vs_pitcher()
    all_df = pd.merge(all_df, head_to_head, how='left', on=['player_id', 'pitcher_id'])

    lineups = get_lineups(today)
    all_df['order'] = all_df.apply(lambda row: lineup_func(lineups, row['player_id'], row['team']), axis = 1)

    all_df = color_columns(all_df[~all_df['opponent'].isnull()], min_hits)
    weather = get_weather()
    all_df['weather'] = all_df['team'].apply(lambda x: weather[x] if x in weather.keys() else '')

    last_updated_str = '{dt:%A} {dt:%B} {dt.day}, {dt.year}'.format(dt = datetime.datetime.strptime(last_date, '%Y-%m-%d'))
    out = jsonify({'data': all_df.fillna('').to_dict('records'), 'lastUpdated': last_updated_str})

    stop_timer('Total', start_time) # Stop timer
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

    today = datetime.datetime.now(tz.gettz('America/Chicago')).date()

    client = pymongo.MongoClient(os.environ.get('DATABASE_CLIENT'))
    database = client[os.environ.get('DATABASE_NAME')]
    collection = database[os.environ.get('DATABASE_COLLECTION')]

    this_year = today.year

    start_date = datetime.date(this_year, 1, 1)
    savant_scrape_days_span = 9
    savant_scrape_date_offset_minus_1, savant_scrape_date_offset = datetime.timedelta(days=savant_scrape_days_span - 1), datetime.timedelta(days=savant_scrape_days_span)

    df_list = list()
    existing_data_df = pd.DataFrame(collection.find())

    last_date = None
    if len(existing_data_df.index) > 0:
        df_list.append(existing_data_df)
        last_date = existing_data_df['game_date'].values[-1]
        print(f'bts_advisor database has data up to {last_date}', '\n', sep='')
        last_date_split = last_date.split('-')
        start_date = datetime.date(this_year, int(last_date_split[1]), int(last_date_split[2])) + datetime.timedelta(days=1)

    while start_date <= today:
        start_date_str, end_date_str = start_date.strftime('%Y-%m-%d'), (start_date + savant_scrape_date_offset_minus_1).strftime('%Y-%m-%d')
        url = 'https://baseballsavant.mlb.com/statcast_search/csv?'
        url_params = [
            'all=true',
            'hfGT=R%7C',
            'hfSea=2021%7C',
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

        df = pd.read_csv(url , usecols=['game_pk', 'game_date', 'away_team', 'home_team', 'inning', 'inning_topbot', 'at_bat_number', 'player_name', 'batter', 'pitcher', 'events', 'stand', 'p_throws', 'estimated_ba_using_speedangle', 'babip_value'])
        obs = len(df.index)
        interval = ' to '.join([start_date_str, end_date_str])
        print(interval, f'{obs} results', sep=': ')

        if obs > 0:
            # Format data
            df = df[(~df['events'].isna()) & (df['events'] != '')] # keep rows that ended at bat
            df['hit'] = df['events'].apply(lambda x: 1 if x in ['home_run', 'triple', 'double', 'single'] else 0)
            df['home_away'] = df['inning_topbot'].apply(lambda x: 'home' if x == 'Bot' else 'away')
            df['team'] = df.apply(lambda row: row['home_team'] if row['home_away'] == 'home' else row['away_team'], axis=1)
            df['opponent'] = df.apply(lambda row: row['home_team'] if row['home_away'] == 'away' else row['away_team'], axis=1)
            df.sort_values(by=['game_date', 'game_pk', 'at_bat_number'], ignore_index=True, inplace=True)
            starters_df = df.groupby(['game_date', 'game_pk', 'opponent']).first().reset_index()[['game_date', 'game_pk', 'opponent', 'pitcher']]
            df = pd.merge(df, starters_df, on=['game_date', 'game_pk', 'opponent', 'pitcher'], how='left', indicator='starter_flg')
            df['starter_flg'] = np.where(df['starter_flg'] == 'both', True, False)
            df.rename({'estimated_ba_using_speedangle': 'xBA', 'stand': 'batter_handedness', 'p_throws': 'pitcher_handedness'}, axis=1, inplace=True)
            df['xBA'].fillna(0, inplace=True)
            df['_id'] = df['game_pk'].astype(str) + '_' + df['at_bat_number'].astype(str)
            df = df[['_id', 'game_date', 'game_pk', 'player_name', 'team', 'opponent', 'batter', 'batter_handedness', 'pitcher', 'pitcher_handedness', 'starter_flg', 'events', 'home_away', 'hit', 'xBA']]
            df_list.append(df)

        start_date += savant_scrape_date_offset

    df = pd.concat(df_list, ignore_index=True)

    # Find games without statcast data
    temp_df = df.groupby(['game_pk', 'team', 'opponent'])['xBA'].sum().reset_index()
    no_statcast_games = temp_df[temp_df['xBA'] == 0]['game_pk'].tolist()
    df['statcast'] = ~df['game_pk'].isin(no_statcast_games)

    out_dict = pd.concat([existing_data_df, df], ignore_index=True).drop_duplicates(subset='_id', keep=False).to_dict('records') # drop duplicate events
    if len(out_dict) > 0: 
        collection.insert_many(out_dict)

    stop_timer('get_statcast_data()', start_time) # Stop timer
    return df, df['game_date'].values[-1]


def calculate_hit_pct(statcast_df, weighted = False):
    start_time = time.time() # Start timer

    keep_cols = ['player_name']

    df_by_game = statcast_df.groupby(['game_date', 'game_pk', 'statcast', 'batter'] + keep_cols)[['hit', 'xBA']].sum().reset_index().rename({'hit': 'H', 'xBA': 'xH'}, axis=1)
    df_by_game['H_1+'] = (df_by_game['H'] >= 1).astype(int)
    df_by_game['G'] = 1
    df_by_game['xH_1+'] = (df_by_game[df_by_game['statcast'] == True]['xH'] >= 1).astype(int)
    df_by_game['statcast_G'] = np.where(df_by_game['statcast'] == True, 1, 0)

    df_by_game['player_id'] = df_by_game['batter']
    keep_cols = ['player_id'] + keep_cols

    agg_func_sum = weighted_sum if weighted == True else 'sum'
    agg_func_avg = weighted_avg if weighted == True else 'mean'
    df_by_season = df_by_game.groupby(keep_cols).agg({'H': 'sum', 'xH': agg_func_avg, 'G': 'sum', 'H_1+': agg_func_sum, 'xH_1+': agg_func_sum, 'statcast_G': 'sum'}).reset_index().rename({'batter': 'G', 'xH': 'xH_per_G'}, axis=1)
    df_by_season['hit_pct'] = df_by_season['H_1+'] / df_by_season['G']
    df_by_season['x_hit_pct'] = df_by_season['xH_1+'] / df_by_season['statcast_G']

    stop_timer('calculate_hit_pct()', start_time) # Stop timer
    return df_by_season.drop(['H_1+', 'xH_1+', 'statcast_G'], axis=1)


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
    percentile_columns_exact = ['H_vs_SP', 'xH_vs_SP']
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


def batter_vs_pitcher():
    start_time = time.time() # Start timer

    html = session.get('https://baseballsavant.mlb.com/daily_matchups', headers = header).text
    data = re.search('(matchups_data\s*=\s*)(\[.*\])', html).group(2)
    df = pd.DataFrame(json.loads(data), columns = ['player_id', 'pitcher_id', 'pa', 'abs', 'hits', 'xba'])
    df['PA_vs_SP'] = df['pa'].fillna(0).astype(int, errors = 'ignore')
    df['abs'] = df['abs'].fillna(0).astype(int, errors = 'ignore')
    df['H_vs_SP'] = df['hits'].fillna(0).astype(int, errors = 'ignore')
    df['xba'] = df['xba'].fillna(np.nan).astype(float, errors = 'ignore')
    df['xH_vs_SP'] = round(df['xba'] * df['abs'], 2).fillna(np.nan)

    stop_timer('batter_vs_pitcher()', start_time) # Stop timer
    return df[['player_id', 'pitcher_id', 'PA_vs_SP', 'H_vs_SP', 'xH_vs_SP']]


def get_lineups(day):
    start_time = time.time() # Start timer

    html = session.get('https://www.mlb.com/starting-lineups/{}'.format(day.strftime('%Y-%m-%d')), headers = header).text
    soup = BeautifulSoup(html, 'lxml')

    out_dict = dict()
    for matchup in soup.find_all('div', {'class': 'starting-lineups__matchup'}):
        away_team = matchup.find('div', {'class': 'starting-lineups__teams--away-head'}).text.strip().split(' ')[0]
        home_team = matchup.find('div', {'class': 'starting-lineups__teams--home-head'}).text.strip().split(' ')[0]
        away_lineup = matchup.find('ol', {'class': 'starting-lineups__team--away'})
        home_lineup = matchup.find('ol', {'class': 'starting-lineups__team--home'})
        for team, lineup in {away_team: away_lineup, home_team: home_lineup}.items():
            slot = 1
            players = lineup.find_all('li')
            if len(players) > 1:
                out_dict[team] = True
                for player in players:
                    link = player.find('a')
                    player_link = link.get('href')
                    player_id = player_link.split('-')[-1]
                    try:
                        out_dict[int(player_id)] = slot
                    except:
                        pass
                    slot += 1
            else:
                out_dict[team] = False

    stop_timer('get_lineups()', start_time) # Stop timer
    return out_dict

def get_player_info(year):
    # Ex. get_player_info(2021)
    # returns df with the following columns: id, fullName, B, T
    start_time = time.time() # Start timer

    player_df1 = pd.read_json(f'https://statsapi.mlb.com/api/v1/sports/1/players?season={year}')
    player_df2 = pd.json_normalize(player_df1['people'])[['id', 'fullName', 'batSide.code', 'pitchHand.code']]

    stop_timer('get_player_info()', start_time) # Stop timer
    return player_df2.rename({'batSide.code': 'B', 'pitchHand.code': 'T'}, axis=1)


def get_schedule(year, date=None):
    # Ex. get_schedule(2021)
    # returns df with the following columns: gamePk, gameDate, gameTime, away, home, awayStarterId, homeStarterId, awayLineup, homeLineup
    start_time = time.time() # Start timer

    date_filter = '' if date == None else '&date={}'.format(datetime.date.strftime(date, '%m/%d/%Y'))
    schedule_url = f'https://statsapi.mlb.com/api/v1/schedule?lang=en&sportId=1&season={year}{date_filter}&gameType=R&hydrate=probablePitcher,lineups'
        
    schedule_df1 = pd.read_json(schedule_url)
    schedule_df2 = pd.json_normalize(schedule_df1['dates'])[['date', 'games']]
    schedule_df3 = schedule_df2.explode('games')
    team_matchups_df = pd.json_normalize(schedule_df3['games'])[['gamePk', 'gameDate', 'teams.away.team.id', 'teams.home.team.id', 'teams.away.probablePitcher.id', 'teams.home.probablePitcher.id', 'lineups.awayPlayers', 'lineups.homePlayers']]
    team_matchups_df['gameDate'], team_matchups_df['gameTime'] = utc_to_central(team_matchups_df['gameDate'])
    team_matchups_df.rename({'teams.away.team.id': 'awayId', 'teams.home.team.id': 'homeId', 'teams.away.probablePitcher.id': 'awayStarterId', 'teams.home.probablePitcher.id': 'homeStarterId', 'lineups.awayPlayers': 'awayLineup', 'lineups.homePlayers': 'homeLineup'}, axis=1, inplace=True)

    teams_df1 = pd.read_json(f'https://statsapi.mlb.com/api/v1/teams?lang=en&sportId=1&season={year}')
    teams_id_map_df = pd.json_normalize(teams_df1['teams'])[['id', 'abbreviation']].set_index('id')
    teams_id_map = teams_id_map_df['abbreviation'].to_dict()
    for side in ['away', 'home']:
        team_matchups_df[side] = team_matchups_df[f'{side}Id'].apply(lambda teamId: teams_id_map[teamId])
        team_matchups_df[f'{side}StarterId'] = team_matchups_df[f'{side}StarterId'].fillna(0).astype(int)
        team_matchups_df[f'{side}Lineup'] = team_matchups_df[f'{side}Lineup'].fillna('').apply(lambda lineup: [batter['id'] for batter in lineup])

    get_schedule('batter_vs_pitcher()', start_time) # Stop timer
    return team_matchups_df.drop(['awayId', 'homeId'], axis=1)


def utc_to_central(time_string_series):
    # Ex. utc_to_central(pd.Series(['2021-04-01T17:05:00Z', '2021-04-01T17:05:10Z']))
    # returns 2 series
    game_time_utc = time_string_series.apply(lambda x: datetime.datetime.strptime(x, '%Y-%m-%dT%H:%M:%SZ').replace(tzinfo=tz.gettz('UTC')))
    game_time_current_time_zone = game_time_utc.apply(lambda x: x.astimezone(tz.gettz('America/Chicago')))
    game_date_current_time_zone_str = game_time_current_time_zone.apply(lambda x: x.strftime('%Y-%m-%d'))
    game_time_current_time_zone_str = game_time_current_time_zone.apply(lambda x: x.strftime('%I:%M %p %Z'))
    return game_date_current_time_zone_str, game_time_current_time_zone_str


def lineup_func(lineups, player_id, team):
    out = 'TBD'
    if player_id in lineups.keys():
        out = lineups[player_id]
    elif team in lineups.keys():
        if lineups[team] == True:
            out = 'OUT'
    return out


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
        if i == 0:
            weights.append(1)
        else:
            weights.append(weights[-1] * 1.1)
    return weights


def stop_timer(function_name, start_time):
    print('\n', '{} time: {}'.format(function_name, datetime.timedelta(seconds = round(time.time() - start_time))), sep='')