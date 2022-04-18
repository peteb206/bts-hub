import pandas as pd
import re
from datetime import datetime
import pymongo
import numpy as np


def sidebar_links_html(db, current_endpoint, collapse_sidebar):
    available_dates = db.get_available_dates()
    year = available_dates[-1].split('-')[0]
    game_dates = [game_date for game_date in available_dates if game_date.startswith(year)]
    list_items = [
        ['Dashboard', 'fas fa-home', '/dashboard'],
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
                        <a class="nav-link" href="#" onclick="showSummary(this)">Split Summary</a>
                    </li>
                    <li class="nav-item">
                        <a class="nav-link" href="https://www.mlb.com/play/games" target="_blank">Make Pick</a>
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

        # players_df = pd.DataFrame(list(db.get_db()['players'].find(
        #     {
        #         'year': date.year
        #     }, {
        #         '_id': False,
        #         'playerId': True,
        #         'playerName': True,
        #         'bats': True
        #     }
        # )))
        # teams_df = pd.DataFrame(list(db.get_db()['teams'].find(
        #     {
        #         'year': date.year
        #     }, {
        #         '_id': False,
        #         'teamId': True,
        #         'teamAbbreviation': True
        #     }
        # )))
        games_df = pd.DataFrame(list(db.get_db()['games'].find(
            {
                'gameDateTimeUTC': {
                    '$gte': datetime(date.year, 1, 1),
                    '$lte': date
                }
            }, {
                '_id': False,
                'awayScore': False,
                'homeScore': False
            }
        )))
        at_bats_df = pd.DataFrame(list(db.get_db()['atBats'].find(
            {
                'gameDateTimeUTC': {
                    '$gte': datetime(date.year, 1, 1),
                    '$lte': date
                }
            }, {
                '_id': False,
                'atBatNumber': False,
                'inning': False
            }
        )))
        event_types_df = pd.DataFrame(list(db.get_db()['eventTypes'].find(
            {}, {
                '_id': False,
                'eventTypeId': True,
                'hitFlag': True,
                'inPlayFlag': True
            }
        )))
        park_factors_df = pd.DataFrame(list(db.get_db()['parkFactors'].find(
            {
                'year': date.year
            }, {
                '_id': False,
                'year': False
            }
        )))

        input_df = pd.merge(at_bats_df.rename({'batterId': 'batter'}, axis=1), event_types_df, how='inner', on=['eventTypeId'])
        input_df = pd.merge(input_df, games_df, how='inner', on=['gamePk', 'gameDateTimeUTC'])
        input_df = pd.merge(input_df, park_factors_df.rename({'rightHandedFlag': 'rightHandedBatterFlag'}, axis=1), how='left', on=['stadiumId', 'rightHandedBatterFlag', 'dayGameFlag'])
        input_df['pitchingTeamId'] = input_df.apply(lambda row: row['awayTeamId'] if row['inningBottomFlag'] else row['homeTeamId'], axis=1)
        input_df['parkFactor'] = input_df['parkFactor'].fillna(100).astype(int)
        input_df['starterFlag'] = input_df.apply(lambda row: row['pitcherId'] == row['awayStarterId' if row['inningBottomFlag'] else 'homeStarterId'], axis=1)
        input_df['batterLineupSlot'] = input_df.apply(lambda row: row['awayLineup'].index(row['batter']) + 1 if row['batter'] in row['awayLineup'] else row['homeLineup'].index(row['batter']) + 1 if row['batter'] in row['homeLineup'] else np.nan, axis=1)
        # input_df.drop(['awayStarterId', 'homeStarterId', 'awayLineup', 'homeLineup', 'stadiumId'], axis=1, inplace=True)
        # df = pd.merge(df, teams_df.rename({'teamId': 'awayTeamId'}, axis=1), how='inner', on=['awayTeamId'])
        # df = pd.merge(df, teams_df.rename({'teamId': 'homeTeamId'}, axis=1), how='inner', on=['homeTeamId'], suffixes=['Away', 'Home'])
        # df = pd.merge(df, players_df.rename({'playerId': 'batter'}, axis=1), how='inner', on=['batter'])
        # df = pd.merge(df, players_df.drop(['bats'], axis=1).rename({'playerId': 'pitcherId'}, axis=1), how='inner', on=['pitcherId'], suffixes=['Batter', 'Pitcher'])

        # Game
        batter_by_game_df = input_df.groupby(['gameDateTimeUTC', 'gamePk', 'statcastFlag', 'inningBottomFlag', 'batter', 'batterLineupSlot']).agg({'hitFlag': 'sum', 'xBA': 'sum'}).reset_index().rename({'xBA': 'xH'}, axis=1)
        batter_by_game_df['HG'] = batter_by_game_df['hitFlag'] >= 1
        batter_by_game_df['xHG'] = batter_by_game_df['xH'] >= 1
        batter_by_game_df['G'] = 1

        def clean_split_columns(df):
            columns, stat = list(), df.columns.names[-1]
            for column in df.columns:
                column_string = None
                if isinstance(column, tuple):
                    column_string = column[0]
                    if stat == 'inningBottomFlag':
                        column_string += ' Home' if column[1] else ' Away'
                    else:
                        if stat == 'rightHandedPitcherFlag':
                            column_string += ' vs RHP' if column[1] else ' vs LHP'
                        elif stat == 'rightHandedBatterFlag':
                            column_string += ' vs RHB' if column[1] else ' vs LHB'
                else:
                    column_string = column
                columns.append(column_string.replace('hitFlag', 'H / PA').replace('xBA', 'xH / PA'))
            df.columns = columns
            return df

        # Season
        batter_seasons_dfs = dict()
        for key, index in {'': ['batter'], 'home/away': ['batter', 'inningBottomFlag']}.items():
            batter_temp_df = batter_by_game_df.groupby(index).agg({'hitFlag': 'mean', 'xH': 'mean', 'HG': 'sum', 'xHG': 'sum', 'batterLineupSlot': 'mean', 'statcastFlag': 'sum', 'G': 'sum'}).rename({'hitFlag': 'H / G', 'xH': 'xH / G', 'statcastFlag': 'statcastG'}, axis=1)
            batter_temp_df['H %'] = batter_temp_df['HG'] / batter_temp_df['G']
            batter_temp_df['xH %'] = batter_temp_df['xHG'] / batter_temp_df['statcastG']
            batter_temp_df = batter_temp_df.drop(['statcastG', 'HG', 'xHG'], axis=1).reset_index()
            if key != '':
                batter_temp_df = clean_split_columns(batter_temp_df.pivot_table(index=['batter'], values=['H / G', 'xH / G', 'G', 'H %', 'xH %'], columns=['inningBottomFlag'])).reset_index()
            batter_seasons_dfs[key] = batter_temp_df
        batter_season_df = batter_seasons_dfs[''] # batter | H / G | xH / G | G | H % | xH %
        batter_home_away_df = batter_seasons_dfs['home/away'] # batter | G Away | G Home | H % Away | H % Home | H / G Away | H / G Home | xH % Away | xH % Home | xH / G Away | xH / G Home

        # Batting splits
        batter_per_pa_vs_bullpen_df = clean_split_columns(input_df.pivot_table(index=['batter'], values=['hitFlag', 'xBA'])).reset_index() # batter | H / PA | xH / PA
        batter_per_pa_vs_rhp_lhp_df = clean_split_columns(input_df.pivot_table(index=['batter'], values=['hitFlag', 'xBA'], columns='rightHandedPitcherFlag')).reset_index() # batter | H / PA vs LHP | H / PA vs RHP | xH / PA vs LHP | xH / PA vs RHP

        # Pitching splits
        # pitcher_per_pa_df = clean_split_columns(input_df.pivot_table(index=['pitcherId'], values=['hitFlag', 'xBA'])).reset_index() # pitcherId | H / PA | xH / PA
        # pitcher_per_pa_vs_rhb_lhp_df = clean_split_columns(input_df.pivot_table(index=['pitcherId'], values=['hitFlag', 'xBA'], columns='rightHandedBatterFlag')).reset_index() # pitcherId | H / PA vs LHB | H / PA vs RHB | xH / PA vs LHB | xH / PA vs RHB
        # bullpen_per_pa_df = clean_split_columns(input_df[input_df['starterFlag'] == False].pivot_table(index=['pitchingTeamId'], values=['hitFlag', 'xBA'])).reset_index()

        def get_lineup_slot(lineups_dict, avg_lineup_slot, game_pk, team_id, player_id):
            slot = f'''<span class="batting-order"><i class="fas fa-circle-question"></i>{f'<span class="buttonText">{round(avg_lineup_slot, 1)}</span>' if avg_lineup_slot >= 0 else ''}</span>'''
            if game_pk in lineups_dict.keys():
                if team_id in lineups_dict[game_pk].keys():
                    if player_id in lineups_dict[game_pk][team_id].keys():
                        slot = lineups_dict[game_pk][team_id][player_id]
                    elif len(lineups_dict[game_pk][team_id].keys()) > 0:
                        slot = '<i class="fas fa-circle-xmark" style="color: red;"></i>'
            return slot

        todays_games, eligible_batters_df = db.get_days_games_from_mlb(date), pd.DataFrame(list(db.eligible_batters(date=date))) # merge this with analytics to provide prediction
        eligible_batters_df = pd.merge(eligible_batters_df, batter_season_df, how='left', on=['batter'])
        eligible_batters_df = pd.merge(eligible_batters_df, batter_home_away_df, how='left', on=['batter'])
        eligible_batters_df = pd.merge(eligible_batters_df, batter_per_pa_vs_bullpen_df, how='left', on=['batter'], suffixes=['', ' vs Bullpen'])
        eligible_batters_df = pd.merge(eligible_batters_df, batter_per_pa_vs_rhp_lhp_df, how='left', on=['batter'])
        eligible_batters_df['lineup'] = eligible_batters_df.apply(lambda row: get_lineup_slot(todays_games['lineups'], row['batterLineupSlot'], row['gamePk'], row['teamId'], row['batter']), axis = 1)
        eligible_batters_df['batter'] = eligible_batters_df.apply(lambda row: f'<a href="javascript:void(0)" class="float-left" onclick="playerView(this, {row["batter"]}, \'batter\')"><i class="fas fa-arrow-circle-right rowSelectorIcon" player-id="{row["batter"]}"></i></a><span class="playerText">{row["name"]}</span>', axis=1)

        html =  f'''
            <div id="mainDashboard">
                <div id="dashboardTablesRow" class="row">
                    <div class="col-5">
                        <div class="row">
                            {html_table('eligibleBatters', eligible_batters_df[['batter', 'team', 'time', 'lineup']], title='Eligible Batters')}
                        </div>
                        <div id="playerImageAndNameRow" class="row selectedPlayer" style="padding-top: 10px;">
                            <div class="col" style="max-width: 125px; margin-right: 10px;">
                                <img id="playerImage" style="width: 125px; height: auto;"/><!-- https://securea.mlb.com/mlb/images/players/head_shot/generic.jpg -->
                            </div>
                            <div id="playerInfo" class="col">
                                <div class="row">
                                    <h4 id="playerName" class="text-center"></h4>
                                </div>
                                <div class="row" style="padding-top: 5px;">
                                    <div class="col">
                                        <span id="playerTeam"></span>
                                    </div>
                                    <div class="col-8">
                                        <span id="fangraphsProjection"></span>
                                    </div>
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
                <div class="row selectedPlayer">
                    <div id="playerGameLogs" class="col">
                    </div>
                </div>
            </div>
            <div id="splitSummary" class="hidden">
                <div class="row">
                    <div id="hitPctByLineup" class="col-6"></div>
                    <div id="otherStatsByLineup" class="col-6"></div>
                </div>
                <div class="row">
                    <div id="hitPctByPAs" class="col-4"></div>
                    <div id="hitPctByHomeAway" class="col-2"></div>
                    <div id="otherStatsByHomeAway" class="col-2"></div>
                    <div id="hitPctByDayNight" class="col-2"></div>
                    <div id="otherStatsByDayNight" class="col-2"></div>
                </div>
                <div class="row">
                    <div id="hitPctByBattingTeam" class="col"></div>
                </div>
                <div class="row">
                    <div id="otherStatsByBattingTeam" class="col"></div>
                </div>
                <div class="row">
                    <div id="hitPctByPitchingTeam" class="col"></div>
                </div>
                <div class="row">
                    <div id="otherStatsByPitchingTeam" class="col"></div>
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
