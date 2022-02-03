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
        filter_html +=       '<span class="buttonText">Go</span>'
        filter_html +=    '</button>'
        filter_html +=    '<button type="button" id="updateFiltersButtonActive" class="btn btn-secondary hidden">'
        filter_html +=       '<i class="fas fa-sync"></i>'
        filter_html +=       '<span class="buttonText">Go</span>'
        filter_html +=    '</button>'
        filter_html += '</div>'
    return filter_html


def html_table(table_id, data):
    assert isinstance(data, (list, pymongo.command_cursor.CommandCursor, pd.DataFrame))
    if isinstance(data, pd.DataFrame):
        data.columns = convert_camel_case_columns(data.columns)
        return data.to_html(na_rep='', table_id=table_id, classes='display', border=0, justify='unset', index=False)
    else:
        tbody, column_list = '', list()
        for document in data:
            if len(column_list) == 0:
                column_list = list(document.keys())
            tbody += '<tr>' + ''.join([f'<td>{format_table_value(document, column)}</td>' for column in column_list]) + '</tr>'
        if len(column_list) == 0:
            column_list = [' ']
        thead = ''.join([f'<th>{column}</th>' for column in convert_camel_case_columns(column_list)])
        return f'<table id="{table_id}" class="display"><thead>{thead}</thead><tbody>{tbody}</tbody></table>'


def convert_camel_case_columns(column_list):
    return [re.sub(r'((?<=[a-z])[A-Z]|(?<!\A)[A-Z](?=[a-z]))', r' \1', (column[0].upper() if column[0] != 'x' else 'x') + column[1:]).replace('x ', 'x') for column in column_list]


def format_table_value(document, column):
    value = document[column]
    if '%' in column:
        value = f'{value} %'
    elif value != value:
        value = ''
    return value


def display_html(db, path, filters={}):
    df, display_columns, current_year = None, None, datetime.now().year
    if path == 'dashboard':
        date = filters['date']
        year = date.year
        players_df = db.read_collection('players', where_dict={'year': year})
        player_id_dict = dict(zip(players_df['playerId'], players_df['playerName']))
        player_id_dict_keys = list(player_id_dict.keys())
        recent_games_df = pd.DataFrame(db.batter_span(where_dict={'gameDateTimeUTC': {'$gte': date - timedelta(days=10), '$lte': date}}))
        recent_games_df['playerName'] = recent_games_df['batterId'].apply(lambda x: player_id_dict[x] if x in player_id_dict_keys else '')
        recent_games_df['H %'] = recent_games_df['H %'].apply(lambda x: f'{round(x * 100, 2)} %')
        recent_games_df['xH / G'] = recent_games_df['xH'].round(2)
        recent_games_df.sort_values(by=['HG', 'H %', 'xH / G'], ascending=False, inplace=True)
        recent_performances_html = html_table('recentPerformances', recent_games_df[['playerName', 'G', 'xH / G', 'H %']])
        todays_games_df = db.read_collection('games', where_dict={'gameDateTimeUTC': {'$gte': date + timedelta(hours=5), '$lte': date + timedelta(hours=30)}})
        todays_games_html = html_table('todaysGames', todays_games_df)
        html =  '<div class="row">'
        html +=    '<div class="col">'
        html +=       '<h4>Recent Performance</h4>'
        html +=       recent_performances_html
        html +=    '</div>'
        html +=    '<div class="col">'
        html +=       "<h4>Today's Games</h4>"
        html +=       todays_games_html
        html +=    '</div>'
        html += '</div>'
        return html
    elif path in ['games', 'atBats']:
        df = db.read_collection(path, where_dict=filters)
        if path == 'games':
            display_columns = ['date', 'time', 'matchup', 'awayStarter', 'homeStarter', 'awayLineup', 'homeLineup', 'stadium', 'statcastTracked?']
        else:
            display_columns = ['batter', 'pitcher', 'matchup', 'lineupSlot', 'inning', 'xBA', 'outcome']
            df = pd.merge(df, db.read_collection('games', where_dict=filters), how='left', on=['gamePk', 'gameDateTimeUTC'])
            df['lineupSlot'] = ''
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
            if path == 'games':
                df[f'{side}Starter'] = df[f'{side}StarterId'].apply(lambda x: player_id_dict[x] if x in player_id_dict_keys else '')
                df[f'{side}Lineup'] = df[f'{side}Lineup'].apply(lambda x: '<ol>{}</ol>'.format(''.join([f'<li>{player_id_dict[player_id]}</li>' if player_id in player_id_dict_keys else '' for player_id in list(x)])))
            else:
                df['lineupSlot'] = df.apply(lambda row: str(row[f'{side}Lineup'].index(row['batterId']) + 1) if (row['lineupSlot'] == '') & (row['batterId'] in row[f'{side}Lineup']) else row['lineupSlot'], axis=1)
            df = pd.merge(df, teams_df, how='left', left_on=['year', f'{side}TeamId'], right_on=['year', 'teamId']).rename({'teamAbbreviation': f'{side}Team'}, axis=1)
        if path == 'atBats':
            df['batter'] = df.apply(lambda row: f'{player_id_dict[row["batterId"]]} ({"R" if row["rightHandedBatterFlag"] else "L"})' if row['batterId'] in player_id_dict_keys else '', axis=1)
            df['pitcher'] = df.apply(lambda row: f'{player_id_dict[row["pitcherId"]]} ({"R" if row["rightHandedPitcherFlag"] else "L"})' if row['pitcherId'] in player_id_dict_keys else '', axis=1)
            df['inning'] = df.apply(lambda row: f'{row["inning"]} ({"Bottom" if row["inningBottomFlag"] else "Top"})', axis=1)
            df = pd.merge(df, db.read_collection('eventTypes').rename({'eventTypeName': 'outcome'}, axis=1), how='left', on='eventTypeId')
        else:
            df = pd.merge(df, db.read_collection('stadiums')[['stadiumId', 'stadiumName']], how='left', on='stadiumId')
        df['matchup'] = df.apply(lambda row: '{} @ {}{}'.format(row['awayTeam'], row['homeTeam'], f' ({row["awayScore"]} - {row["homeScore"]})' if row['awayScore'] + row['homeScore'] >= 0 else ''), axis=1)
        df.rename({'statcastFlag': 'statcastTracked?', 'stadiumName': 'stadium'}, axis=1, inplace=True)
    elif path == 'players':
        date_filter = {'gameDateTimeUTC': {'$gte': datetime(filters['year'], 1, 1), '$lte': datetime(filters['year'], 12, 31)}} if 'year' in filters.keys() else dict()
        return html_table('dataView', db.batter_span(where_dict=date_filter))
    elif path == 'teams':
        date_filter = {'gameDateTimeUTC': {'$gte': datetime(filters['year'], 1, 1), '$lte': datetime(filters['year'], 12, 31)}} if 'year' in filters.keys() else dict()
        return html_table('dataView', db.team_span(where_dict=date_filter))
    elif path == 'stadiums':
        display_columns = ['stadiumName', 'rightyDayParkFactor', 'leftyDayParkFactor', 'rightyNightParkFactor', 'leftyNightParkFactor']
        park_factor_filter = dict()
        if 'year' in filters.keys():
            park_factor_filter['year'] = filters['year']
        pf_df = db.read_collection('parkFactors', where_dict=park_factor_filter)
        pf_df = pf_df.pivot_table(index=['year', 'stadiumId'], columns=['rightHandedFlag', 'dayGameFlag'], values='parkFactor', aggfunc='first').reset_index()
        pf_df.columns = [f'{"righty" if col[0] else "lefty"}{"Day" if col[1] else "Night"}ParkFactor' if isinstance(col[0], bool) & isinstance(col[1], bool) else col[0] for col in pf_df.columns]
        df = pd.merge(db.read_collection(path), pf_df, how='right', on='stadiumId')
        df.sort_values(by=['rightyNightParkFactor', 'leftyNightParkFactor'], ascending=False, inplace=True)
    else:
        return ''
    df[' '] = '<span><i class="fas fa-arrow-circle-right rowSelectorIcon"></i></span>'
    return html_table('dataView', df[[' '] + display_columns])
