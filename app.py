from flask import Flask, jsonify, render_template, request
import pandas as pd
import numpy as np
import requests
import json
import os
import sys
import datetime
import time
import pymongo


app = Flask(__name__)


@app.route('/')
def home():
   return render_template('index.html')


@app.route("/loadTableData")
def index():
    today = datetime.date.today()
    statcast_df = get_statcast_data(today)

    general_info_df = statcast_df.drop_duplicates(subset='batter', keep='last').rename({'batter': 'player_id'}, axis=1)[['player_id', 'team', 'batter_handedness']]
    calculations_df = calculate_hit_pct(statcast_df)
    last_x_days = int(request.args.get('days'))
    x_days_ago = today - datetime.timedelta(days=last_x_days + 1)
    calculations_df_x_days = calculate_hit_pct(statcast_df, since_date=x_days_ago.strftime('%Y-%m-%d'))
    calculations_df_x_days.drop('player_name', axis=1, inplace=True)

    calculations_df = pd.merge(general_info_df, calculations_df, how='right', on='player_id')
    all_df = pd.merge(calculations_df, calculations_df_x_days, how='left', on='player_id', suffixes=('_total', f'_{last_x_days}'))
    all_df = pd.merge(all_df, get_opponent_info(statcast_df, today), how='left', left_on='team', right_on='opponent').drop(['game_number', 'opponent'], axis=1).rename({'pitching_team': 'opponent'}, axis=1)
    all_df['opponent'] = np.where(all_df['home_away'] == 'away', all_df['opponent'], '@' + all_df['opponent'])
    all_df['player_name'] = all_df['player_name'].apply(lambda name: ' '.join(name.split(',')[::-1]).strip())

    pd.set_option('expand_frame_repr', False)
    # print('\n', 'Season and Recent expected Hit-Game % >= 50%:', '\n', '\n', all_df[(all_df['x_hit_pct_total'] >= 0.5) & (all_df['x_hit_pct_{}'.format(last_x_days)] >= 0.5)], sep='')
    out_dict = all_df[~all_df['opponent'].isnull()].fillna('').to_dict('records')
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
    return df


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
        teams = game['teams']
        for home_away in ['away', 'home']:
            team = teams[home_away]
            team_abbreviation = team['team']['abbreviation']
            opposing_team_abbreviation = teams['home' if home_away == 'away' else 'away']['team']['abbreviation']
            matchup_dict = dict()
            matchup_dict['team'] = team_abbreviation
            matchup_dict['opponent'] = opposing_team_abbreviation
            matchup_dict['home_away'] = home_away
            matchup_dict['game_number'] = game_number
            if 'probablePitcher' in team.keys():
                pitcher_id = team['probablePitcher']['id']
                matchup_dict['pitcher_id'] = pitcher_id
                matchup_dict['pitcher_name'] = team['probablePitcher']['firstLastName']
                matchup_dict['sp_xHA_per_BF_interval']  = get_pitcher_stats(statcast_df[(statcast_df['pitcher'] == pitcher_id) & (statcast_df['starter_flg'] == True)])
            matchup_dict['bp_xHA_per_BF_interval'] = get_pitcher_stats(statcast_df[(statcast_df['opponent'] == team_abbreviation) & (statcast_df['starter_flg'] == False)])
            matchups.append(matchup_dict)
    matchups_df = pd.DataFrame(matchups).rename({'team': 'pitching_team'}, axis=1)
    return matchups_df


def get_pitcher_stats(df, since_date=None):
    if since_date != None:
        df = df[df['game_date'] >= since_date]
    df_grouped = df.groupby(['game_date', 'game_pk'])
    xHA = df_grouped.agg({'xBA': ['sum', 'mean']}).reset_index()
    xHA.columns = ['_'.join(col) for col in xHA.columns.values]
    xHA.rename({'xBA_sum': 'xHA', 'xBA_mean': 'xHA_per_BF'}, axis=1, inplace=True)
    if len(xHA.index):
        q1, q2, q3 = np.percentile(xHA['xHA_per_BF'], 25), np.percentile(xHA['xHA_per_BF'], 50), np.percentile(xHA['xHA_per_BF'], 75)
        tup = round(q1, 2), round(q2, 2), round(q3, 2)
        return str(tup)
    else:
        return ''