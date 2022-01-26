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


def sidebar_links_html(db, current_endpoint, collapse_sidebar):
    # Font awesome icons: https://fontawesome.com/v5.0/icons?d=gallery&p=2&m=free
    year = db.date.year
    end_date = datetime.datetime.now()
    start_date = end_date - datetime.timedelta(days=14)
    start_date, end_date = start_date.strftime('%Y-%m-%d'), end_date.strftime('%Y-%m-%d')
    list_items = [
        ['Dashboard', 'fa-home', '/dashboard'],
        ['My Picks', 'fa-pencil-alt', '/picks'],
        ['Leaderboard', 'fa-list-ol', '/leaderboard'],
        ['Simulation', 'fa-chart-bar', '/simulations'],
        ['Games', 'fa-calendar-alt', f'/games?year={year}'],
        ['At Bats', 'fa-baseball-ball', f'/atBats?startDate={start_date}&endDate={end_date}'],
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
        active = current_endpoint == list_item[2].split('?')[0]
        list_items_html += '<li title="' + list_item[0] + '"' + (' class="active">' if active else '>')
        list_items_html +=    '<a href="' + ('#' if active else list_item[2]) + '">'
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


def filters_html(current_path, filter_type, filter_values):
    filter_html = '<div class="col-3">'
    if filter_type == 'date':
        date_value = filter_values.strftime('%a, %B %-d, %Y')
        filter_html +=  '<span class="datePickerLabel">Date:</span>'
        filter_html += f'<input placeholder="None selected..." type="text" id="date" class="datepicker" value="{date_value}">'
    elif filter_type == 'range':
        date_value = [filter_value.strftime('%a, %B %-d, %Y') if filter_value else '' for filter_value in filter_values]
        filter_html +=  '<span class="datePickerLabel">Start Date:</span>'
        filter_html += f'<input placeholder="None selected..." type="text" id="startDate" class="datepicker" value="{date_value[0]}">'
        filter_html += '</div>'
        filter_html +=  '<div class="col-3">'
        filter_html +=  '<span class="datePickerLabel">End Date:</span>'
        filter_html += f'<input placeholder="None selected..." type="text" id="endDate" class="datepicker" value="{date_value[1]}">'
    elif filter_type == 'year':
        filter_html += '<span class="datePickerLabel">Year:</span>'
        filter_html += '<select id="endDatePicker" onchange="location=this.value;">'
        for year in range(2015, 2023):
            date_value = f'/{current_path}?year={year}'
            selected_year = 'selected="selected"' if year == filter_values else ''
            filter_html += f'<option value="{date_value}" {selected_year}>{year}</option>'
        filter_html += '</select>'
    filter_html += '</div>'
    return filter_html


def html_table(table_id, data):
    assert isinstance(data, (list, pd.DataFrame))
    if isinstance(data, list):
        tbody, column_list = '', list()
        for document in data:
            if len(column_list) == 0:
                column_list = list(document.keys())
            tbody += '<tr>' + ''.join([f'<td>{format_table_value(document[column])}</td>' for column in column_list]) + '</tr>'
        if len(column_list) == 0:
            column_list = [' ']
        thead = ''.join(['<th>{}</th>'.format(re.sub(r'((?<=[a-z])[A-Z]|(?<!\A)[A-Z](?=[a-z]))', r' \1', column[0].upper() + column[1:])) for column in column_list])
        return f'<table id="{table_id}" class="display"><thead>{thead}</thead><tbody>{tbody}</tbody></table>'
    else:
        return data.to_html(na_rep='', table_id=table_id, classes='display', border=None, justify='unset', index=False)


def format_table_value(value):
    if value != value:
        value = ''
    return value