try:
    import app.utils as utils
except ModuleNotFoundError:
    import utils
import pandas as pd
import re
from datetime import datetime, timedelta
import pymongo


def sidebar_links_html(db, current_endpoint, collapse_sidebar):
    available_dates = db.get_available_dates()
    year = available_dates[-1].split('-')[0]
    game_dates = [game_date for game_date in available_dates if game_date.startswith(year)]
    list_items = [
        ['Dashboard', 'fas fa-home', '/dashboard'],
        # ['My Picks', 'fas fa-pencil-alt', '/picks'],
        # ['Leaderboard', 'fas fa-list-ol', '/leaderboard'],
        ['Splits', 'fas fa-coins' , '/splits'],
        ['Simulations', 'fas fa-chart-bar', '/simulations'],
        ['Data', 'fas fa-database', '/dataView'],
        ['Games', 'fas fa-calendar-alt', f'/games?startDate={game_dates[0]}&endDate={game_dates[-1]}'],
        ['At Bats', 'fas fa-baseball-ball', f'/atBats?startDate={available_dates[-7]}&endDate={available_dates[-1]}'],
        ['Players', 'fas fa-users', f'/players?year={year}'],
        ['Teams', 'fas fa-trophy', f'/teams?year={year}'],
        ['Stadiums', 'fas fa-university', f'/stadiums?year={year}'],
        # ['About', 'fas fa-question-circle', '/links']
    ]
    list_items_html, full_sidebar_hidden, partial_sidebar_hidden  = '', '', ''
    if collapse_sidebar:
        full_sidebar_hidden = ' hidden'
    else:
        partial_sidebar_hidden = ' hidden'
    for list_item in list_items:
        active = current_endpoint == list_item[2].split('?')[0]
        list_items_html += '<li title="' + list_item[0] + '"' + ' class="' + ('active' if active else 'inactive') + '">'
        list_items_html +=    '<a href="' + ('#' if active else list_item[2]) + '">'
        list_items_html +=       '<div class="fullSidebarTab' + full_sidebar_hidden + '">'
        list_items_html +=          '<i class="' + list_item[1] + '"></i>'
        list_items_html +=          '<span class="buttonText">' + list_item[0] + '</span>'
        list_items_html +=       '</div>'
        list_items_html +=       '<div class="container-fluid partialSidebarTab' + partial_sidebar_hidden + '">'
        list_items_html +=          '<i class="' + list_item[1] + ' fa-lg"></i>'
        list_items_html +=       '</div>'
        list_items_html +=    '</a>'
        list_items_html += '</li>'
    return list_items_html


def filters_html(filter_types, filter_values):
    filter_html = ''
    if 'date' in filter_types:
        date_value = filter_values.strftime('%a, %B %-d, %Y')
        filter_html += '<div class="col-auto">'
        filter_html +=    '<span class="datePickerLabel">Date:</span>'
        filter_html +=   f'<input placeholder="None selected..." type="text" id="date" class="datepicker" value="{date_value}" readonly>'
        filter_html += '</div>'
    elif 'date_range' in filter_types:
        date_value = [filter_value.strftime('%a, %B %-d, %Y') if filter_value else '' for filter_value in filter_values]
        filter_html += '<div class="col-auto">'
        filter_html +=    '<span class="datePickerLabel">Start Date:</span>'
        filter_html +=   f'<input placeholder="None selected..." type="text" id="startDate" class="datepicker" value="{date_value[0]}" readonly>'
        filter_html += '</div>'
        filter_html += '<div class="col-auto">'
        filter_html +=    '<span class="datePickerLabel">End Date:</span>'
        filter_html +=   f'<input placeholder="None selected..." type="text" id="endDate" class="datepicker" value="{date_value[1]}" readonly>'
        filter_html += '</div>'
    elif 'year' in filter_types:
        filter_html += '<div class="col-auto">'
        filter_html +=    '<span class="datePickerLabel">Year:</span>'
        filter_html +=    '<select id="yearPicker" onchange="addClass($(\'#updateFiltersButtonInactive\'), \'hidden\'); removeClass($(\'#updateFiltersButtonActive\'), \'hidden\');">'
        for year in range(2015, 2023):
            selected_year = ' selected="selected"' if year == filter_values else ''
            filter_html +=    f'<option value="{year}"{selected_year}>{year}</option>'
        filter_html +=    '</select>'
        filter_html += '</div>'
    if filter_html != '':
        filter_html += '<div class="col-auto">'
        filter_html +=    '<button type="button" id="updateFiltersButtonInactive" class="btn btn-secondary">'
        filter_html +=       '<i class="fas fa-sync"></i>'
        filter_html +=       '<span class="buttonText refreshFilter">Go</span>'
        filter_html +=    '</button>'
        filter_html +=    '<button type="button" id="updateFiltersButtonActive" class="btn btn-secondary hidden">'
        filter_html +=       '<i class="fas fa-sync"></i>'
        filter_html +=       '<span class="buttonText refreshFilter">Go</span>'
        filter_html +=    '</button>'
        filter_html += '</div>'
    return filter_html


def html_table(table_id, data):
    assert isinstance(data, (list, pymongo.command_cursor.CommandCursor, pd.DataFrame))
    if isinstance(data, pd.DataFrame):
        data.columns = convert_camel_case_columns(data.columns)
        return data.to_html(na_rep='', table_id=table_id, classes='display nowrap', border=0, justify='unset', index=False)
    else:
        tbody, column_list = '', list()
        for document in data:
            if len(column_list) == 0:
                column_list = list(document.keys())
            tbody += '<tr>' + ''.join([f'<td>{format_table_value(document, column)}</td>' for column in column_list]) + '</tr>'
        if len(column_list) == 0:
            column_list = [' ']
        thead = ''.join([f'<th>{column}</th>' for column in convert_camel_case_columns(column_list)])
        return f'<table id="{table_id}" class="display nowrap"><thead>{thead}</thead><tbody>{tbody}</tbody></table>'


def convert_camel_case_columns(column_list):
    return [re.sub(r'((?<=[a-z])[A-Z]|(?<!\A)[A-Z](?=[a-z]))', r' \1', (column[0].upper() if column[0] != 'x' else 'x') + column[1:]).replace('x ', 'x') for column in column_list]


def format_table_value(document, column):
    value = ''
    try:
        value = document[column]
        if '%' in column:
            value = f'{value} %'
        elif value != value:
            value = ''
    except KeyError:
        pass
    return value


def display_html(db, path, filters={}):
    html = ''
    if path == 'dashboard':
        date = filters['date']
        html =  '<div class="row">'
        html +=    '<div class="col-6">'
        html +=       '<h4>Recent Performance</h4>'
        html +=       html_table('recentPerformances', db.recent_batter_performances(date=date))
        html +=    '</div>'
        html +=    '<div class="col-6">'
        html +=       "<h4>Today's Games</h4>"
        html +=       html_table('todaysGames', db.dashboard_games(date=date))
        html +=    '</div>'
        html += '</div>'
    elif path == 'games':
        html = html_table('dataView', db.game_span(where_dict=filters))
    elif path == 'atBats':
        html = html_table('dataView', db.at_bat_span(where_dict=filters))
    elif path == 'players':
        html = html_table('dataView', db.batter_span(where_dict={'gameDateTimeUTC': {'$gte': datetime(filters['year'], 1, 1), '$lte': datetime(filters['year'], 12, 31)}}))
    elif path == 'teams':
        html = html_table('dataView', db.team_span(where_dict={'gameDateTimeUTC': {'$gte': datetime(filters['year'], 1, 1), '$lte': datetime(filters['year'], 12, 31)}}))
    elif path == 'stadiums':
        html = html_table('dataView', db.stadium_park_factors(year=filters['year']))
    return html
