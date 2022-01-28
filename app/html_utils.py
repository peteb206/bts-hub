try:
    import app.utils as utils
except ModuleNotFoundError:
    import utils
from numpy import where
import pandas as pd
import re

from app.utils import utc_to_central


def sidebar_links_html(db, current_endpoint, collapse_sidebar):
    available_dates = utils.get_available_dates(db)
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
        ['At Bats', 'fas fa-baseball-ball', f'/atBats?startDate={available_dates[-10]}&endDate={available_dates[-1]}'],
        ['Players', 'fas fa-users', f'/players?year={year}'],
        ['Teams', 'fas fa-trophy', f'/teams?year={year}'],
        ['Stadiums', 'fas fa-university', f'/stadiums?year={year}'],
        # ['Links', 'fas fa-link', '/links']
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
        filter_html +=       '<span class="buttonText">Update</span>'
        filter_html +=    '</button>'
        filter_html +=    '<button type="button" id="updateFiltersButtonActive" class="btn btn-secondary hidden">'
        filter_html +=       '<i class="fas fa-sync"></i>'
        filter_html +=       '<span class="buttonText">Update</span>'
        filter_html +=    '</button>'
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
        thead = ''.join([f'<th>{column}</th>' for column in convert_camel_case_columns(column_list)])
        return f'<table id="{table_id}" class="display"><thead>{thead}</thead><tbody>{tbody}</tbody></table>'
    else:
        data.columns = convert_camel_case_columns(data.columns)
        df = data.to_html(na_rep='', table_id=table_id, classes='display', border=0, justify='unset', index=False)
        return df


def convert_camel_case_columns(column_list):
    return [re.sub(r'((?<=[a-z])[A-Z]|(?<!\A)[A-Z](?=[a-z]))', r' \1', column[0].upper() + column[1:]) for column in column_list]


def format_table_value(value):
    if value != value:
        value = ''
    return value


def display_html(db, path, filters={}):
    html = ''
    if path == 'games':
        display_columns = ['date', 'time', 'matchup', 'awayStarter', 'homeStarter', 'awayLineup', 'homeLineup', 'stadium', 'statcastTracked?']
        df = db.read_collection(path, where_dict=filters)
        df['year'] = df['gameDateTimeUTC'].apply(lambda x: x.year)
        df['date'] = df['gameDateTimeUTC'].apply(lambda x: utils.utc_to_central(x, return_type='date'))
        df['time'] = df['gameDateTimeUTC'].apply(lambda x: utils.utc_to_central(x, return_type='time'))
        year_list = [int(year) for year in df['year'].unique()]
        year_filter = {'year': {'$in': year_list}}
        teams_df = db.read_collection('teams', where_dict=year_filter)[['year', 'teamId', 'teamAbbreviation']]
        players_df = db.read_collection('players', where_dict=year_filter).drop_duplicates(subset='playerId')
        player_id_dict = dict(zip(players_df['playerId'], players_df['playerName']))
        player_id_dict_keys = list(player_id_dict.keys())
        for side in ['away', 'home']:
            df = pd.merge(df, teams_df, how='left', left_on=['year', f'{side}TeamId'], right_on=['year', 'teamId']).rename({'teamAbbreviation': f'{side}Team'}, axis=1)
            df[f'{side}Starter'] = df[f'{side}StarterId'].apply(lambda x: player_id_dict[x] if x in player_id_dict_keys else '')
            df[f'{side}Lineup'] = df[f'{side}Lineup'].apply(lambda x: ' | '.join([f'{(k + 1)}. {v}' for k, v in enumerate([player_id_dict[player_id] if player_id in player_id_dict_keys else '' for player_id in list(x)])]))
        df['matchup'] = df.apply(lambda row: f'{row["awayTeam"]} @ {row["homeTeam"]}', axis=1)
        df['stadium'] = ''
        df.rename({'statcastFlag': 'statcastTracked?'}, axis=1, inplace=True)
        html = html_table('dataView', df[display_columns])
    elif path == 'stadiums':
        park_factor_filter, display_columns = dict(), ['stadiumName', 'time', 'batterHand', 'parkFactor']
        if 'year' in filters.keys():
            park_factor_filter['year'] = filters['year']
        else:
            display_columns = ['year'] + display_columns
        park_factors_df = db.read_collection('parkFactors', where_dict=park_factor_filter)
        park_factors_df['time'] = park_factors_df['dayGameFlag'].apply(lambda x: 'Day' if x else 'Night')
        park_factors_df['batterHand'] = park_factors_df['rightHandedFlag'].apply(lambda x: 'Right' if x else 'Left')
        df = pd.merge(db.read_collection(path), park_factors_df, how='right', on='stadiumId')
        df.sort_values(by='parkFactor', ascending=False, inplace=True)
        html = html_table('dataView', df[display_columns])
    else:
        html = db.read_collection_to_html_table(path, where_dict=filters)
    return html