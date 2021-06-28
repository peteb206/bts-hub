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


app = Flask(__name__)


@app.route('/')
def home():
   return render_template('index.html')


@app.route("/loadTableData")
def index():
    last_x_days = int(request.args.get('days'))
    min_hits = int(request.args.get('hitMin'))

    today = datetime.date.today()
    statcast_data = get_statcast_data(today)
    statcast_df = statcast_data[0]
    last_date = statcast_data[1]

    general_info_df = statcast_df.drop_duplicates(subset=['batter', 'batter_handedness'], keep='last').rename({'batter': 'player_id'}, axis=1)[['player_id', 'team', 'batter_handedness']]
    general_info_df['batter_handedness'] = np.where(general_info_df['player_id'].duplicated(keep=False), 'B', general_info_df['batter_handedness'])
    general_info_df.drop_duplicates(subset='player_id', inplace=True)

    calculations_df = calculate_hit_pct(statcast_df)
    x_days_ago = today - datetime.timedelta(days=last_x_days + 1)
    calculations_df_x_days = calculate_hit_pct(statcast_df, since_date=x_days_ago.strftime('%Y-%m-%d'))
    calculations_df_x_days.drop('player_name', axis=1, inplace=True)

    calculations_df = pd.merge(general_info_df, calculations_df, how='right', on='player_id')
    all_df = pd.merge(calculations_df, calculations_df_x_days, how='left', on='player_id', suffixes=('_total', f'_{last_x_days}'))
    all_df = pd.merge(all_df, get_opponent_info(statcast_df, today), how='left', left_on='team', right_on='opponent').drop(['game_number', 'opponent'], axis=1).rename({'pitching_team': 'opponent'}, axis=1)
    all_df['opponent'] = np.where(all_df['home_away'] == 'away', all_df['opponent'], '@' + all_df['opponent'])
    all_df['opponent'] = all_df['opponent'] + ' (' + all_df['game_time'] + ')'
    all_df['player_name'] = all_df['player_name'].apply(lambda name: ' '.join(name.split(',')[::-1]).strip())

    pd.set_option('expand_frame_repr', False)
    # print('\n', 'Season and Recent expected Hit-Game % >= 50%:', '\n', '\n', all_df[(all_df['x_hit_pct_total'] >= 0.5) & (all_df['x_hit_pct_{}'.format(last_x_days)] >= 0.5)], sep='')
    all_df = color_columns(all_df[~all_df['opponent'].isnull()], min_hits, last_x_days)
    out_dict = dict()
    out_dict['data'] = all_df.fillna('').to_dict('records')
    out_dict['lastUpdated'] = last_date
    return jsonify(out_dict)


def get_statcast_data(today):
    # Start timer
    start_time = time.time()

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
            'hfPT=',
            'hfAB=',
            'hfGT=R%7C',
            'hfPR=',
            'hfZ=',
            'stadium=',
            'hfBBL=',
            'hfNewZones=',
            'hfPull=',
            'hfC=',
            'hfSea=2021%7C',
            'hfSit=',
            'player_type=batter',
            'hfOuts=',
            'opponent=',
            'pitcher_throws=',
            'batter_stands=',
            'hfSA=',
            'game_date_gt={}'.format(start_date_str),
            'game_date_lt={}'.format(end_date_str),
            'hfInfield=',
            'team=',
            'position=',
            'hfOutfield=',
            'hfRO=',
            'home_road=',
            'hfFlag=',
            'hfBBT=',
            'metric_1=',
            'hfInn=',
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

    # Stop timer
    print('\n', 'Done retrieving statcast data!', '\n', '\n', '--- Total time: {} minutes ---'.format(str(round((time.time() - start_time) / 60, 2))), sep='')
    return df, df['game_date'].values[-1]


def calculate_hit_pct(statcast_df, since_date=None):
    keep_cols = ['player_name']
    if since_date != None:
        statcast_df = statcast_df[statcast_df['game_date'] >= since_date]
    df_by_game = statcast_df.groupby(['game_pk', 'statcast', 'batter'] + keep_cols)[['hit', 'xBA']].sum().reset_index().rename({'hit': 'H', 'xBA': 'xH'}, axis=1)
    df_by_game['H_1+'] = (df_by_game['H'] >= 1).astype(int)
    df_by_game['G'] = 1
    df_by_game['xH_1+'] = (df_by_game[df_by_game['statcast'] == True]['xH'] >= 1).astype(int)
    df_by_game['statcast_G'] = np.where(df_by_game['statcast'] == True, 1, 0)

    df_by_game['player_id'] = df_by_game['batter']
    keep_cols = ['player_id'] + keep_cols

    df_by_season = df_by_game.groupby(keep_cols).agg({'H': 'sum', 'xH': 'mean', 'G': 'sum', 'H_1+': 'sum', 'xH_1+': 'sum', 'statcast_G': 'sum'}).reset_index().rename({'batter': 'G', 'xH': 'xH_per_G'}, axis=1)
    df_by_season['hit_pct'] = df_by_season['H_1+'] / df_by_season['G']
    df_by_season['x_hit_pct'] = df_by_season['xH_1+'] / df_by_season['statcast_G']
    return df_by_season.drop(['H_1+', 'xH_1+', 'statcast_G'], axis=1)


def get_opponent_info(statcast_df, today):
    matchups = list()
    url = 'https://baseballsavant.mlb.com/schedule?date={}'.format(today.strftime('%Y-%m-%d'))
    response_json = requests.get(url).json()
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
                matchup_dict['sp_HA_per_BF_total'], matchup_dict['sp_xHA_per_BF_total']  = get_pitcher_stats(statcast_df[(statcast_df['pitcher'] == pitcher_id) & (statcast_df['starter_flg'] == True)])
            matchup_dict['bp_HA_per_BF_total'], matchup_dict['bp_xHA_per_BF_total'] = get_pitcher_stats(statcast_df[(statcast_df['opponent'] == team_abbreviation) & (statcast_df['starter_flg'] == False)])
            matchups.append(matchup_dict)
    matchups_df = pd.DataFrame(matchups).rename({'team': 'pitching_team'}, axis=1)
    return matchups_df


def get_pitcher_stats(df, since_date=None):
    if since_date != None:
        df = df[df['game_date'] >= since_date]
    if len(df.index):
        return round(df['hit'].mean(), 2), round(df['xBA'].mean(), 2)
    else:
        return np.nan, np.nan


def color_columns(df, min_hits, last_x_days):
    df_new = df.copy()
    df_new = df_new[(df_new['H_total'] >= min_hits) & (df_new['H_{}'.format(last_x_days)] >= 1)]
    rwg = ['#F8696B', '#F86B6D', '#F86E70', '#F87173', '#F87476', '#F87779', '#F87A7C', '#F87D7F', '#F88082', '#F88385', '#F88688', '#F8898B', '#F88C8E', '#F98F91', '#F99294', '#F99597', '#F9989A', '#F99A9D', '#F99DA0', '#F9A0A3', '#F9A3A6', '#F9A6A9', '#F9A9AC', '#F9ACAF', '#F9AFB2', '#FAB2B5', '#FAB5B7', '#FAB8BA', '#FABBBD', '#FABEC0', '#FAC1C3', '#FAC4C6', '#FAC7C9', '#FACACC', '#FACCCF', '#FACFD2', '#FAD2D5', '#FAD5D8', '#FBD8DB', '#FBDBDE', '#FBDEE1', '#FBE1E4', '#FBE4E7', '#FBE7EA', '#FBEAED', '#FBEDF0', '#FBF0F3', '#FBF3F6', '#FBF6F9', '#FBF9FC', '#FCFCFF', '#F9FBFD', '#F6FAFA', '#F3F9F8', '#F0F8F5', '#EDF6F2', '#EAF5F0', '#E7F4ED', '#E4F3EA', '#E1F1E8', '#DEF0E5', '#DBEFE2', '#D8EEE0', '#D5ECDD', '#D2EBDB', '#CFEAD8', '#CCE9D5', '#C8E7D3', '#C5E6D0', '#C2E5CD', '#BFE4CB', '#BCE2C8', '#B9E1C5', '#B6E0C3', '#B3DFC0', '#B0DDBD', '#ADDCBB', '#AADBB8', '#A7DAB6', '#A4D9B3', '#A1D7B0', '#9ED6AE', '#9BD5AB', '#98D4A8', '#94D2A6', '#91D1A3', '#8ED0A0', '#8BCF9E', '#88CD9B', '#85CC99', '#82CB96', '#7FCA93', '#7CC891', '#79C78E', '#76C68B', '#73C589', '#70C386', '#6DC283', '#6AC181', '#67C07E', '#63BE7B']
    gwr = rwg[::-1]

    percentile_columns = ['H', 'xH_per_G', 'hit_pct', 'x_hit_pct', 'sp_HA_per_BF', 'sp_xHA_per_BF', 'bp_HA_per_BF', 'bp_xHA_per_BF']
    descending_columns = []
    for column in percentile_columns:
        for column_subset in ['total', str(last_x_days)]:
            column_w_subset = column + '_' + column_subset
            if (column_w_subset in df_new.columns):
                df_new[column_w_subset + '_color'] = df_new[column_w_subset].fillna(0).rank(pct=True)
                df_new[column_w_subset + '_color'] = df_new[column_w_subset + '_color'].apply(lambda x: gwr[math.floor(x * 100)] if column in descending_columns else rwg[math.floor(x * 100)])
    return df_new