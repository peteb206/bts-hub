import datetime
import time
from dateutil import tz
import numpy as np
import json
import pandas as pd
import re


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


def sidebar_links(db, current_endpoint, collapse_sidebar):
    # Font awesome icons: https://fontawesome.com/v5.0/icons?d=gallery&p=2&m=free
    year = db.date.year
    list_items = [
        ['Dashboard', 'fa-home', '/dashboard'],
        ['My Picks', 'fa-pencil-alt', '/picks'],
        ['Leaderboard', 'fa-list-ol', '/leaderboard'],
        ['Simulation', 'fa-chart-bar', '/simulations'],
        ['Games', 'fa-baseball-ball', f'/games?year={year}'],
        ['Players', 'fa-users', f'/players?year={year}'],
        ['Teams', 'fa-trophy', f'/teams?year={year}'],
        ['Stadiums', 'fa-university', '/stadiums'],
        ['Links', 'fa-link', '/links']
    ]
    list_items_html, full_sidebar_hidden, partial_sidebar_hidden  = '', '', ''
    if collapse_sidebar:
        full_sidebar_hidden = ' hidden'
    else:
        partial_sidebar_hidden = ' hidden'
    for list_item in list_items:
        list_items_html += '<li title="' + list_item[0] + '"' + (' class="active">' if current_endpoint == list_item[2] else '>')
        list_items_html +=    '<a href="' + (list_item[2] if current_endpoint != list_item[2] else '#') + '">'
        list_items_html +=       '<div class="fullSidebarTab' + full_sidebar_hidden + '">'
        list_items_html +=          '<i class="fas ' + list_item[1] + '"></i>'
        list_items_html +=          '<span class="buttonText">' + list_item[0] + '</span>'
        list_items_html +=       '</div>'
        list_items_html +=       '<div class="container-fluid partialSidebarTab' + partial_sidebar_hidden + '">'
        list_items_html +=          '<i class="fas ' + list_item[1] + ' fa-lg"></i>'
        list_items_html +=       '</div>'
        list_items_html +=    '</a>'
        list_items_html += '</li>'
    return list_items_html


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


def html_table(table_id, data):
    assert isinstance(data, (list, pd.DataFrame))
    if isinstance(data, list):
        tbody, column_list = '', None
        for document in data:
            if not column_list:
                column_list = list(document.keys())
            tbody += '<tr>' + ''.join([f'<td>{document[column]}</td>' for column in column_list]) + '</tr>'
        thead = ''.join(['<th>{}</th>'.format(re.sub(r'((?<=[a-z])[A-Z]|(?<!\A)[A-Z](?=[a-z]))', r' \1', column[0].upper() + column[1:])) for column in column_list])
        return f'<table id="{table_id}" class="display"><thead>{thead}</thead><tbody>{tbody}</tbody></table>'
    else:
        html_table_no_styling = data.to_html(table_id=table_id, index=False).replace('border="1"', '').replace(' style="text-align: right;"', '').replace('"dataframe"', '"display"') # remove pandas styling
        return re.sub(r'\s{2,}', ' ', html_table_no_styling).replace('\n', '').replace('> <', '><') # remove unneeded whitespace