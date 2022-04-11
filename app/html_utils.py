import pandas as pd
import re
from datetime import datetime
import pymongo


def sidebar_links_html(db, current_endpoint, collapse_sidebar):
    available_dates = db.get_available_dates()
    year = available_dates[-1].split('-')[0]
    game_dates = [game_date for game_date in available_dates if game_date.startswith(year)]
    list_items = [
        ['Dashboard', 'fas fa-home', '/'],
        # ['My Picks', 'fas fa-pencil-alt', '/picks'],
        # ['Leaderboard', 'fas fa-list-ol', '/leaderboard'],
        # ['Splits', 'fas fa-coins' , '/splits'],
        # ['Simulations', 'fas fa-chart-bar', '/simulations'],
        # ['Data', 'fas fa-database', '/dataView'],
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
        list_items_html += f'''
            <li title="{list_item[0]}" class="{'active' if active else 'inactive'}">
                <a href="{'#' if active else list_item[2]}">
                    <div class="fullSidebarTab{full_sidebar_hidden}">
                        <i class="{list_item[1]}"></i>
                        <span class="buttonText">{list_item[0]}</span>
                    </div>
                    <div class="container-fluid partialSidebarTab{partial_sidebar_hidden}">
                        <i class="{list_item[1]} fa-lg"></i>
                    </div>
                </a>
            </li>
        '''
    return list_items_html


def filters_html(path, filter_types, filter_values):
    filter_html = ''
    if 'date' in filter_types:
        date_value = filter_values.strftime('%a, %B %-d, %Y')
        filter_html = f'''
            <div class="col-auto">
                <input placeholder="Select a date..." type="text" id="date" class="datepicker" value="{date_value}" readonly>
            </div>
        '''
    elif 'date_range' in filter_types:
        date_value = [filter_value.strftime('%a, %B %-d, %Y') if filter_value else '' for filter_value in filter_values]
        filter_html = f'''
            <div class="col-auto">
                <span class="datePickerLabel">From:</span>
                <input placeholder="Select a date..." type="text" id="startDate" class="datepicker" value="{date_value[0]}" readonly>
            </div>
            <div class="col-auto">
                <span class="datePickerLabel">To:</span>
                <input placeholder="Select a date..." type="text" id="endDate" class="datepicker" value="{date_value[1]}" readonly>
            </div>
        '''
    elif 'year' in filter_types:
        filter_html = f'''
            <div class="col-auto">
                <span class="datePickerLabel">Year:</span>
                <select id="yearPicker" onchange="$('#updateFiltersButton').prop('disabled', false);">
                    {''.join([f'<option value="{year}"' + (' selected="selected"' if year == filter_values else '') + f'>{year}</option>' for year in range(2015, 2023)])}
                </select>
            </div>
        '''
    if filter_html != '':
        filter_html += '''
            <div class="col-auto">
                <button type="button" id="updateFiltersButton" class="btn btn-secondary" disabled>
                    <i class="fas fa-sync"></i>
                    <span class="buttonText refreshFilter">Go</span>
                </button>
            </div>
        '''
    if path == 'dashboard':
        filter_html += '''
            <div id="dashboardTabs" class="col-auto offset-md-1">
                <ul class="nav nav-tabs" role="tab-list">
                    <li class="nav-item">
                        <a class="nav-link active" href="#" onclick="showMainDashboard(this)">Today</a>
                    </li>
                    <li class="nav-item">
                        <a class="nav-link" href="#" onclick="showSeasonSummary(this)">Season Summary</a>
                    </li>
                </ul>
            </div>
        '''
    return filter_html


def html_table(table_id, data, title=None):
    assert isinstance(data, (list, pymongo.command_cursor.CommandCursor, pd.DataFrame))
    title = f'<caption>{title}</caption>' if title else ''
    if isinstance(data, pd.DataFrame):
        data.columns = convert_camel_case_columns(data.columns)
        return data.to_html(na_rep='', table_id=table_id, classes='display nowrap', border=0, justify='unset', index=False).replace('<thead', f'{title}<thead')
    else:
        tbody, column_list = '', list()
        for document in data:
            if len(column_list) == 0:
                column_list = list(document.keys())
            tbody += '<tr>' + ''.join([f'<td>{format_table_value(document, column)}</td>' for column in column_list]) + '</tr>'
        if len(column_list) == 0:
            column_list = [' ']
        thead = ''.join([f'<th>{column}</th>' for column in convert_camel_case_columns(column_list)])
        return f'<table id="{table_id}" class="display nowrap">{title}<thead>{thead}</thead><tbody>{tbody}</tbody></table>'


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

        todays_games = db.get_days_games_from_mlb(date)

        def get_lineup_slot(lineups_dict, game_pk, team_id, player_id):
            slot = '<i class="fas fa-circle-question"></i>'
            if game_pk in lineups_dict.keys():
                if team_id in lineups_dict[game_pk].keys():
                    if player_id in lineups_dict[game_pk][team_id].keys():
                        slot = lineups_dict[game_pk][team_id][player_id]
                    elif len(lineups_dict[game_pk][team_id].keys()) > 0:
                        slot = '<i class="fas fa-circle-xmark" style="color: red;"></i>'
            return slot

        eligible_batters_df = pd.DataFrame(list(db.eligible_batters(date=date))) # merge this with analytics to provide prediction
        eligible_batters_df['lineup'] = eligible_batters_df.apply(lambda row: get_lineup_slot(todays_games['lineups'], row['gamePk'], row['teamId'], row['batter']), axis = 1)
        eligible_batters_df['batter'] = eligible_batters_df.apply(lambda row: f'<a href="javascript:void(0)" class="float-left" onclick="playerView(this, {row["batter"]}, \'batter\')"><i class="fas fa-arrow-circle-right rowSelectorIcon"></i></a><span class="playerText">{row["name"]}</span>', axis=1)

        html =  f'''
            <div id="mainDashboard">
                <div id="dashboardTablesRow" class="row">
                    <div class="col-5">
                        <div class="row">
                            {html_table('eligibleBatters', eligible_batters_df[['batter', 'team', 'time', 'lineup']], title='Eligible Batters')}
                        </div>
                        <div id="playerImageAndNameRow" class="row" style="padding-top: 10px;">
                            <div class="col" style="max-width: 125px; margin-right: 10px;">
                                <img id="playerImage" style="width: 125px; height: auto;"/><!-- https://securea.mlb.com/mlb/images/players/head_shot/generic.jpg -->
                            </div>
                            <div id="playerInfo" class="col">
                                <div class="row">
                                    <h4 id="playerName" class="text-center"></h4>
                                </div>
                                <div class="row" style="padding-top: 5px;">
                                    <span id="playerTeam"></span>
                                </div>
                                <div class="row" style="padding-top: 5px;">
                                    <span id="playerPosition"></span>
                                </div>
                                <div class="row" style="padding-top: 5px;">
                                    <span id="playerBats"></span>
                                </div>
                                <div class="row" style="padding-top: 5px;">
                                    <span id="playerThrows"></span>
                                </div>
                            </div>
                        </div>
                    </div>
                    <div class="col-7">
                        <div class="row">
                            {html_table('todaysGames', todays_games['games'], title="Today's Games")}
                        </div>
                    </div>
                </div>
                <div class="row">
                    <div id="playerGameLogs" class="col">
                    </div>
                </div>
            </div>
            <div id="seasonSummary" class="row">
                <h5 class="text-center">Productivity by Batting Order Spot</h5>
                <div id="seasonSummaryPct" class="col-6">
                </div>
                <div id="seasonSummaryOth" class="col-6">
                </div>
            </div>
        '''
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
