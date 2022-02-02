import datetime
import time
from dateutil import tz
import numpy as np
import json
import pandas as pd


def stop_timer(function_name, start_time):
    print('{} time: {}'.format(function_name, datetime.timedelta(seconds = round(time.time() - start_time))))


def utc_to_central(game_time_utc, return_type='time'):
    game_time_current_time_zone = game_time_utc.tz_localize('UTC').astimezone(tz.gettz('America/Chicago'))
    datetime_string = game_time_current_time_zone.strftime('%I:%M %p %Z' if return_type == 'time' else '%Y-%m-%d')
    return datetime_string[1:] if (return_type == 'time') & (datetime_string[0] == '0') else datetime_string


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


def parse_request_arguments(args):
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


def get_available_dates(db, max_min=None):
    pipeline = [
        {
            '$match': {
                'gameDateTimeUTC': {
                    '$lte': datetime.datetime.combine(db.date + datetime.timedelta(days=1), datetime.datetime.max.time())
                }
            }
        }, {
            '$project': {
                key: {f'${key}': '$gameDateTimeUTC'} for key in ['year', 'month', 'dayOfMonth']
            }
        }, {
            '$group': {
                '_id': {
                    'year': '$year',
                    'month': '$month',
                    'day': '$dayOfMonth'
                }
            }
        }, {
            '$sort': {
                '_id': -1 if max_min == 'max' else 1
            }
        }
    ]
    return_one = False
    if max_min in ['min', 'max']:
        pipeline.append({
            '$limit': 1
        })
        return_one = True
    result = [f'{date["_id"]["year"]}-{str(date["_id"]["month"]).zfill(2)}-{str(date["_id"]["day"]).zfill(2)}' for date in db.get_db()['games'].aggregate(pipeline)]
    return result[0] if return_one else result


def pipeline_builder(where_dict={}, group_list=[], agg_list=[], extra_stages_list=[]):
    group_dict, group_id = dict(), dict()
    for group in group_list:
        group_id[group] = f'${group}'
        group_dict[group] = {
            '$first': f'${group}'
        }
    group_dict['_id'] = group_id
    for agg in agg_list:
        group_dict[agg['column']] = {
            f'${agg["agg_operator"]}': agg['agg_function']
        }
    pipeline = list()
    if len(where_dict.keys()) > 0:
        pipeline.append({
            '$match': where_dict
        })
    pipeline.append({
        '$group': group_dict
    })
    pipeline.append({
        '$unset': '_id'
    })
    if len(extra_stages_list) > 0:
        pipeline += extra_stages_list
    return pipeline


def batter_games(db, where_dict={}):
    event_types = db.read_collection_as_list('eventTypes')
    hit_event_type_ids = [event_type['eventTypeId'] for event_type in event_types if event_type['hitFlag']]
    agg_list = [
        {
            'column': 'xBA',
            'agg_operator': 'sum',
            'agg_function': {
                '$cond': [
                    {
                        '$gte': [
                            '$xBA',
                            0
                        ]
                    },
                    '$xBA',
                    0
                ]
            }
        }, {
            'column': 'hits',
            'agg_operator': 'sum',
            'agg_function': {
                '$cond': [
                    {
                        '$in': [
                            '$eventTypeId',
                            hit_event_type_ids
                        ]
                    },
                    1,
                    0
                ]
            }
        }
    ]
    pipeline = pipeline_builder(where_dict=where_dict, group_list=['gamePk', 'batterId'], agg_list=agg_list)
    return list(db.get_db()['atBats'].aggregate(pipeline))


def batter_spans(db, where_dict={}):
    event_types = db.read_collection_as_list('eventTypes')
    hit_event_type_ids = [event_type['eventTypeId'] for event_type in event_types if event_type['hitFlag']]
    games_agg_list = [
        {
            'column': 'xBA',
            'agg_operator': 'sum',
            'agg_function': {
                '$cond': [
                    {
                        '$gte': [
                            '$xBA',
                            0
                        ]
                    },
                    '$xBA',
                    0
                ]
            }
        }, {
            'column': 'hits',
            'agg_operator': 'sum',
            'agg_function': {
                '$cond': [
                    {
                        '$in': [
                            '$eventTypeId',
                            hit_event_type_ids
                        ]
                    },
                    1,
                    0
                ]
            }
        }
    ]
    batters_agg_list = [
        {
            'column': 'G',
            'agg_operator': 'sum',
            'agg_function': 1
        }, {
            'column': 'HG',
            'agg_operator': 'sum',
            'agg_function': {
                '$cond': [
                    {
                        '$gte': [
                            '$hits',
                            1
                        ]
                    },
                    1,
                    0
                ]
            }
        }, {
            'column': 'xH',
            'agg_operator': 'avg',
            'agg_function': '$xBA'
        }
    ]
    extra_stages_list = [
        {
            '$addFields': {
                'H %': {
                    '$divide': [
                        '$HG',
                        '$G'
                    ]
                }
            }
        }
    ]
    games_pipeline = pipeline_builder(where_dict=where_dict, group_list=['gamePk', 'batterId'], agg_list=games_agg_list)
    batters_pipeline = pipeline_builder(group_list=['batterId'], agg_list=batters_agg_list, extra_stages_list=extra_stages_list)
    pipeline = games_pipeline + batters_pipeline
    return list(db.get_db()['atBats'].aggregate(pipeline))


def team_games(db, where_dict={}):
    event_types = db.read_collection_as_list('eventTypes')
    hit_event_type_ids = [event_type['eventTypeId'] for event_type in event_types if event_type['hitFlag']]
    games_agg_list = [
        {
            'column': 'xBA',
            'agg_operator': 'sum',
            'agg_function': {
                '$cond': [
                    {
                        '$gte': [
                            '$xBA',
                            0
                        ]
                    },
                    '$xBA',
                    0
                ]
            }
        }, {
            'column': 'hits',
            'agg_operator': 'sum',
            'agg_function': {
                '$cond': [
                    {
                        '$in': [
                            '$eventTypeId',
                            hit_event_type_ids
                        ]
                    },
                    1,
                    0
                ]
            }
        }
    ]
    pipeline = pipeline_builder(where_dict=where_dict, group_list=['gamePk', 'gameDateTimeUTC', 'inningBottomFlag'], agg_list=games_agg_list)
    return list(db.get_db()['atBats'].aggregate(pipeline))