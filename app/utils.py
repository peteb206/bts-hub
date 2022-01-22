import datetime
import time
from dateutil import tz
import numpy as np
import json
import pandas as pd


def stop_timer(function_name, start_time):
    print('{} time: {}'.format(function_name, datetime.timedelta(seconds = round(time.time() - start_time))))


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


def game_time_func(game, is_today):
    game_time = ''
    if game['status']['statusCode'] == 'I':
        game_time = game['status']['detailedState']
        if 'linescore' in game.keys():
            linescore = game['linescore']
            linescore_keys = linescore.keys()
            if 'currentInningOrdinal' in linescore_keys:
                game_time = linescore['currentInningOrdinal']
            if 'inningHalf' in linescore_keys:
                game_time = '{} {}'.format(game['linescore']['inningHalf'], game_time)
    elif (is_today == False) | (game['status']['statusCode'] in ['F', 'O', 'UR', 'CR']) | ('D' in game['status']['statusCode']):
        game_time = game['status']['detailedState']
    else:
        game_time = utc_to_central(game['gameDate'], 'time')
        if game_time[0] == '0':
            game_time = game_time[1:]
    return game_time


def weighted_avg(s):
    return np.average(s, weights = calculate_weights(s)) if len(s) > 0 else np.nan


def weighted_sum(s):
    s2 = s.dropna()
    out = np.nan
    if len(s2) > 0:
        weights = calculate_weights(s2)
        out =  np.dot(s2, weights) / np.mean(weights)
    return out


def calculate_weights(s):
    weights = list()
    for i in range(len(s)):
        weights.append(1 if i == 0 else weights[-1] * 1.1)
    return weights


def merge(base_df, dfs):
    df_num = 1
    suffixes = ['', '']
    for df_tuple in dfs:
        suffixes[1] = str(df_num)
        df, how, on, index = df_tuple
        if index == True:
            base_df = pd.merge(base_df, df, how=how, left_index=True, right_index=True, suffixes=suffixes)
        else:
            base_df = pd.merge(base_df, df, how=how, on=on, suffixes=suffixes)
        df_num += 1
    return base_df


def int_columns(df, columns):
    for col in columns:
        if col in df.columns:
            df[col] = df[col].fillna(0).astype(int)
    return df


def string_columns(df, columns):
    for col in columns:
        if col in df.columns:
            df[col] = df[col].fillna('').astype(str)
    return df


def parseRequestArguments(args):
    args_dict = dict()
    for key, value in args.items():
        if key != '_':
            if value.isdigit():
                value = int(value)
            elif value.replace('.', '', 1).isdigit():
                value = float(value)
            elif (value[0] == '{') & (value[-1] == '}'):
                try:
                    value = json.loads(value)
                except json.decoder.JSONDecodeError:
                    value = value
            args_dict[key] = value
    return args_dict