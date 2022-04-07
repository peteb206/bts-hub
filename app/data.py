try:
    import app.html_utils as html_utils
except ModuleNotFoundError:
    import html_utils
import os
import sys
import pymongo
import requests
import pandas as pd
import json
from bs4 import BeautifulSoup
from requests_html import HTMLSession
from datetime import datetime, date as dt, timedelta
import time
from pandas.api.types import is_datetime64_any_dtype as is_datetime


class BTSHubMongoDB:
    def __init__(self, database_client, database_name, date=None):
        # Set up database connection
        self.__db = pymongo.MongoClient(database_client)[database_name]
        # Set up base urls
        self.__stats_api_url = 'https://statsapi.mlb.com/api/v1'
        self.__stats_api_url_ext = f'{self.__stats_api_url}/sports/1'
        self.__stats_api_default_params = 'lang=en&sportId=1'
        # Set dates
        today = datetime.utcnow().date()
        self.date = date if date else today
        self.__today = today
        # Set info for baseball savant
        self.input_events = list()
        self.output_events = dict()
        # Request settings
        self.session = requests.Session()
        self.fangraphs_header = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/88.0.4324.182 Safari/537.36',
            'Referer': 'https://www.fangraphs.com/roster-resource/injury-report'
        }


    ####################################
    ############# Helpers ##############
    ####################################
    def get_db(self):
        return self.__db


    def __add_to_db(self, collection, records):
        assert isinstance(records, (pd.DataFrame, list))
        if isinstance(records, list):
            pass
        elif isinstance(records, pd.DataFrame):
            records = records.to_dict('records')
        else:
            return None
        self.get_db()[collection].insert_many(records)
        return f'Added {len(records)} new record(s) to {collection}.'


    def __update_db(self, collection, existing_df, new_df, return_collection=False):
        if len(existing_df.columns) * len(existing_df.index) == 0: # Empty database collection
            print(self.__add_to_db(collection, new_df))
            print(new_df)
        else:
            # Find primary key(s) and column(s) of collection
            indices, pks = self.get_db()[collection].index_information(), list()
            for index, info in indices.items():
                if index != '_id_':
                    for pk_tup in info['key']:
                        pks.append(pk_tup[0])
            columns, change = [col for col in existing_df.columns if col not in pks], False
            existing_df = self.__add_year_column(existing_df)
            existing_df['_new'] = False
            new_df = self.__add_year_column(new_df)
            new_df['_new'] = True
            combined_df = pd.concat([existing_df, new_df]).drop_duplicates(subset=pks + columns, keep=False, ignore_index=True)
            no_pk_dups_df, query_string = combined_df.drop_duplicates(subset=pks, keep=False), '(not _new)'
            if ('year' in existing_df.columns) & ('year' in combined_df.columns):
                query_string += f' & (year == {self.date.year})'
            new_records_df, old_records_df = no_pk_dups_df.query('_new'), no_pk_dups_df.query(query_string)
            if len(new_records_df.index) > 0:
                new_records_df, change = new_records_df[pks + columns], True
                print(self.__add_to_db(collection, new_records_df))
                print(new_records_df)
            updated_records_df = combined_df[combined_df['_new'] & combined_df.duplicated(subset=pks)].copy()
            if len(updated_records_df.index) > 0:
                updated_records_df, change = updated_records_df[pks + columns], True
                print(self.__update_records(collection, pks, columns, updated_records_df))
                print(updated_records_df)
            if len(old_records_df.index) > 0:
                old_records_df = old_records_df[pks + columns]
                print(f'The following records were not found in the scrape for {collection}:')
                print(old_records_df)
            if not change:
                print(f'No records added to or updated in {collection}.')

        # Update lastUpdate time in lastUpdate collection
        self.get_db()['lastUpdate'].update_one(
            {
                'collection': collection
            }, {
                '$set': {
                    'lastUpdate': datetime.utcnow()
                }
            },
            upsert = True
        )
        if return_collection:
            return self.read_collection(collection)


    def __update_records(self, collection, pks, columns, records):
        assert isinstance(records, (pd.DataFrame, list))
        if isinstance(records, pd.DataFrame):
            records = records.to_dict('records')
        if isinstance(records, list):
            updated, added = 0, 0
            for record in records:
                update = self.get_db()[collection].update_one(
                    {
                        pk: record[pk] for pk in pks
                    }, {
                        '$set': {
                            col: record[col] for col in columns
                        }
                    },
                    upsert = True
                )
                if update.matched_count == 0:
                    added += 1
                else:
                    updated += 1
        return f'Added {added} new record(s) and updated {updated} existing record(s) in {collection}.'


    def __get(self, url):
        request = requests.get(url)
        return json.loads(request.text)


    def get_statcast_games(self):
        agg = self.get_db()['atBats'].aggregate(
            [
                {
                    '$match': {
                        'xBA': {
                            '$ne': float('NaN') # filter out NaN xBA values
                        }
                    }
                }, {
                    '$group': {
                        '_id': {
                            'gamePk': '$gamePk',
                            'gameDateTimeUTC': '$gameDateTimeUTC'
                        },
                        'xBA': {
                            '$sum': "$xBA"
                        }
                    }
                }, {
                    '$match': {
                        'xBA': {
                            '$gt': 0 # return games with xBA sum greater than 0
                        }
                    }
                }
            ]
        )
        df = pd.DataFrame(list(agg))
        df['gamePk'] = df['_id'].apply(lambda x: x['gamePk'])
        df['gameDate'] = df['_id'].apply(lambda x: (x['gameDateTimeUTC'] - timedelta(hours=5)).replace(hour=0, minute=0, second=0)) # This should help align with game dates
        df.drop(['_id', 'xBA'], axis=1, inplace=True)
        df['statcastFlag'] = True
        return df


    def injured_player_ids(self):
        load_date = self.session.get(f'https://www.fangraphs.com/api/roster-resource/injury-report/loaddate?season={self.date.year}', headers = self.fangraphs_header).text.strip('\"')
        injuries = json.loads(self.session.get(f'https://cdn.fangraphs.com/api/roster-resource/injury-report/data?loaddate={load_date}&season={self.date.year}', headers = self.fangraphs_header).text)
        out = [injury['mlbamid'] for injury in injuries if injury['returndate'] == None]
        return out


    def __add_year_column(self, df):
        datetime_cols = [column for column in df.columns if is_datetime(df[column])]
        if len(datetime_cols) == 1:
            df['year'] = df[datetime_cols[0]].apply(lambda x: x.year)
        return df
    ####################################
    ########### End Helpers ############
    ####################################


    ####################################
    ########## Get From Web ############
    ####################################
    def get_eventTypes_from_mlb(self):
        # Read example html from Baseball Savant
        session = HTMLSession()
        r = session.get('https://baseballsavant.mlb.com/statcast_search?hfPT=&hfAB=triple%5C.%5C.play%7C&hfGT=R%7C&hfPR=&hfZ=&stadium=&hfBBL=&hfNewZones=&hfPull=&hfC=&hfSea=2021%7C&hfSit=&player_type=pitcher&hfOuts=&opponent=&pitcher_throws=&batter_stands=&hfSA=&game_date_gt=&game_date_lt=&hfInfield=&team=&position=&hfOutfield=&hfRO=&home_road=&hfFlag=&hfBBT=&metric_1=&hfInn=&min_pitches=0&min_results=0&group_by=name&sort_col=pitches&player_event_sort=api_p_release_speed&sort_order=desc&min_pas=0#results')
        r.html.render(sleep=10, timeout=10)

        # Calculated columns
        hit_list = ['single', 'double', 'triple', 'home..run']
        ball_in_play_list = hit_list + ['field..out', 'double..play', 'field..error', 'grounded..into..double..play', 'fielders..choice', 'fielders..choice..out', 'force..out', 'sac..bunt', 'sac..bunt..double..play', 'sac..fly', 'sac..fly..double..play', 'triple..play']
        soup = BeautifulSoup(r.html.html, 'html.parser')
        session.close()
        events_list, i = list(), 1
        for event_input in soup.find_all('input', {'class': 'ms_class_AB'}):
            event = event_input['id'].split('_')[-1]
            events_list.append({
                'eventTypeId': i,
                'inputEventType': event.replace('..', ' '),
                'outputEventType': event.replace('..', '_'),
                'eventTypeName': event_input.parent.find('label').text.strip(),
                'inPlayFlag': event in ball_in_play_list,
                'hitFlag': event in hit_list
            })
            i += 1
        df = pd.DataFrame(events_list)

        # Clean up dataframe
        return df[['eventTypeId', 'inputEventType', 'outputEventType', 'eventTypeName', 'inPlayFlag', 'hitFlag']]


    def get_stadiums_from_mlb(self):
        # Read json
        stadiums_dict = self.__get(f'{self.__stats_api_url}/venues')
        stadiums_df = pd.DataFrame(stadiums_dict['venues'])[['id', 'name']]

        # Clean up dataframe
        stadiums_df.rename({'id': 'stadiumId', 'name': 'stadiumName'}, axis=1, inplace=True)
        stadiums_df.sort_values(by='stadiumId', ignore_index=True, inplace=True)
        return stadiums_df[['stadiumId', 'stadiumName']]


    def get_parkFactors_from_mlb(self):
        session, park_factors_list = HTMLSession(), list()
        for day_night in ['Day', 'Night']:
            for right_left in ['R', 'L']:
                r = session.get(f'https://baseballsavant.mlb.com/leaderboard/statcast-park-factors?type=venue&batSide={right_left}&stat=index_Hits&condition={day_night}&rolling=no')
                r.html.render(sleep=10, timeout=10)
                soup = BeautifulSoup(r.html.html, 'html.parser')
                table = soup.find_all('table')[-1]
                thead, tbody = table.find('thead'), table.find('tbody')
                column_headers = [column_header.text for column_header in thead.find_all('th')]
                for trow in tbody.find_all('tr'):
                    col_num, stadium_id = 0, None
                    for td in trow.find_all('td'):
                        column_name, td_text = column_headers[col_num], td.text.strip()
                        if column_name == 'Venue':
                            a = td.find('a', href=True)
                            if a:
                                stadium_id = int(a['href'].split('=')[-1])
                        elif (stadium_id != None) & (td_text != ''):
                            park_factors_list.append({
                                'year': int(column_name),
                                'stadiumId': stadium_id,
                                'dayGameFlag': day_night == 'Day',
                                'rightHandedFlag': right_left == 'R',
                                'parkFactor': int(td_text)
                            })
                        col_num += 1
        session.close()
        df = pd.DataFrame(park_factors_list)
        df.sort_values(by=['year', 'stadiumId', 'dayGameFlag', 'rightHandedFlag'], inplace=True)
        return df


    def get_teams_from_mlb(self):
        # Read json
        teams_dict = self.__get(f'{self.__stats_api_url}/teams?{self.__stats_api_default_params}&season={self.date.year}')
        teams_df = pd.DataFrame(teams_dict['teams'])[['season', 'id', 'abbreviation', 'name', 'division']]

        # Calculated columns
        teams_df['divisionName'] = teams_df['division'].apply(lambda x: ''.join([y[0] if y in ['American', 'National', 'League'] else f' {y}' for y in x['name'].split()]))

        # Clean up dataframe
        teams_df.rename({'season': 'year', 'id': 'teamId', 'abbreviation': 'teamAbbreviation', 'name': 'teamName'}, axis=1, inplace=True)
        teams_df.sort_values(by=['year', 'divisionName', 'teamId'], ignore_index=True, inplace=True)
        return teams_df[['year', 'teamId', 'teamAbbreviation', 'teamName', 'divisionName']]


    def get_players_from_mlb(self):
        # Read json
        year = self.date.year
        players_dict = self.__get(f'{self.__stats_api_url_ext}/players?{self.__stats_api_default_params}&season={year}')
        players_df = pd.DataFrame(players_dict['people'])[['id', 'fullName', 'currentTeam', 'primaryPosition', 'batSide', 'pitchHand']]

        # Calculated columns
        players_df['year'] = year
        players_df['teamId'] = players_df['currentTeam'].apply(lambda x: int(x['id']))
        players_df['position'] = players_df['primaryPosition'].apply(lambda x: x['abbreviation'])
        players_df['bats'] = players_df['batSide'].apply(lambda x: x['code'])
        players_df['throws'] = players_df['pitchHand'].apply(lambda x: x['code'])
        injured_players_list = self.injured_player_ids() if self.__today.year == self.date.year else list()
        players_df['injuredFlag'] = players_df['id'].apply(lambda x: x in injured_players_list)

        # Clean up dataframe
        players_df.rename({'id': 'playerId', 'fullName': 'playerName'}, axis=1, inplace=True)
        players_df.sort_values(by=['year', 'teamId', 'playerId'], ignore_index=True, inplace=True)
        return players_df[['year', 'playerId', 'teamId', 'playerName', 'position', 'bats', 'throws', 'injuredFlag']]


    def get_days_games_from_mlb(self, date):
        # Read json
        games_dict = self.__get(f'{self.__stats_api_url}/schedule?{self.__stats_api_default_params}&gameType=R&date={date.strftime("%Y-%m-%d")}&hydrate=team,probablePitcher,lineups,weather')

        # Weather icons
        def get_icon(weather):
            weather_icon = ''
            if 'condition' in weather.keys():
                icon_map =  {
                    'clear': 'fa fa-sun',
                    'sunny': 'fas fa-sun',
                    'partly cloudy': 'fas fa-cloud-sun',
                    'cloudy': 'fas fa-cloud',
                    'overcast': 'fas fa-cloud',
                    'rain': 'fas fa-cloud-rain',
                    'roof closed': 'fas fa-people-roof',
                    'dome': 'fas fa-people-roof'
                }
                weather_key, weather_icon = weather['condition'].lower(), weather['condition']
                if weather_key in icon_map.keys():
                    weather_icon = f'''
                        <span title="{weather["condition"]}">
                            <i class="{icon_map[weather_key]} weatherIcon"></i>
                        </span>
                    '''
                if 'temp' in weather.keys():
                    weather_icon += f'<span>{weather["temp"]} &#186;F</span>'
            return weather_icon

        def get_status(game):
            status = game['status']['detailedState']
            if 'score' in game['teams']['away'].keys():
                status += f' ({game["teams"]["away"]["score"]} - {game["teams"]["home"]["score"]})'
            return status

        # Calculated
        games_list, lineups_dict = list(), dict()
        for game_date in games_dict['dates']:
            for game in game_date['games']:
                lineups_dict[game['gamePk']] = dict()
                for side in ['away', 'home']:
                    lineups_dict[game['gamePk']][game['teams'][side]['team']['id']] = dict()
                    if 'lineups' in game.keys():
                        if f'{side}Players' in game['lineups'].keys():
                            i = 1
                            for player in game['lineups'][f'{side}Players']:
                                lineups_dict[game['gamePk']][game['teams'][side]['team']['id']][player['id']] = i
                                i += 1
                games_list.append({
                    'time': (datetime.strptime(game['gameDate'], '%Y-%m-%dT%H:%M:%SZ') - timedelta(hours=5)).strftime('%H:%M'),
                    'matchup': f'{game["teams"]["away"]["team"]["abbreviation"]} @ {game["teams"]["home"]["team"]["abbreviation"]}',
                    'awayStarter': f'<a href="javascript:void(0)" class="float-left" onclick="playerView(this, {game["teams"]["away"]["probablePitcher"]["id"]}, \'pitcher\')"><i class="fas fa-arrow-circle-right rowSelectorIcon"></i></a><span class="playerText">{game["teams"]["away"]["probablePitcher"]["fullName"]}</span>' if 'probablePitcher' in game['teams']['away'].keys() else '',
                    'homeStarter': f'<a href="javascript:void(0)" class="float-left" onclick="playerView(this, {game["teams"]["home"]["probablePitcher"]["id"]}, \'pitcher\')"><i class="fas fa-arrow-circle-right rowSelectorIcon"></i></a><span class="playerText">{game["teams"]["home"]["probablePitcher"]["fullName"]}</span>' if 'probablePitcher' in game['teams']['home'].keys() else '',
                    'status': get_status(game),
                    'weather': get_icon(game['weather'])
                })

        return {
            'games': games_list,
            'lineups': lineups_dict
        }


    def get_games_from_mlb(self):
        # Read json
        games_dict = self.__get(f'{self.__stats_api_url}/schedule?{self.__stats_api_default_params}&gameType=R&season={self.date.year}&hydrate=probablePitcher,lineups,weather')

        # Calculated columns
        games_list = list()
        for date in games_dict['dates']:
            games_list += date['games']
        games_df = pd.DataFrame(games_list)
        games_df = games_df[[col for col in ['gameDate', 'officialDate', 'gamePk', 'teams', 'lineups', 'venue', 'dayNight', 'weather'] if col in games_df.columns]]
        for col in ['lineups', 'weather']:
            games_df[col] = games_df.apply(lambda row: dict() if col not in row.keys() else row[col] if isinstance(row[col], dict) else dict(), axis=1)
        for side in ['away', 'home']:
            games_df[f'{side}TeamId'] = games_df['teams'].apply(lambda x: x[side]['team']['id'])
            games_df[f'{side}Score'] = games_df['teams'].apply(lambda x: x[side]['score'] if 'score' in x[side].keys() else -1)
            games_df[f'{side}StarterId'] = games_df['teams'].apply(lambda x: x[side]['probablePitcher']['id'] if 'probablePitcher' in x[side].keys() else 0)
            games_df[f'{side}Lineup'] = games_df['lineups'].apply(lambda x: tuple(player['id'] for player in x[f'{side}Players']) if f'{side}Players' in x.keys() else tuple())
        games_df['gameDateTimeUTC'] = games_df['gameDate'].apply(lambda x: datetime.strptime(x, '%Y-%m-%dT%H:%M:%SZ'))
        games_df['gameDate'] = games_df['gameDateTimeUTC'].apply(lambda x: (x - timedelta(hours=5)).replace(hour=0, minute=0, second=0)) # This should help align with statcast dates
        games_df['dayGameFlag'] = games_df['dayNight'] == 'day'
        games_df['stadiumId'] = games_df['venue'].apply(lambda x: x['id'])
        games_df = pd.merge(games_df, self.get_statcast_games(), how='left', on=['gamePk', 'gameDate'])
        games_df['statcastFlag'].fillna(False, inplace=True)
        games_df['temperature'] = games_df['weather'].apply(lambda x: x['temp'] if 'temp' in x.keys() else '')
        games_df['weather'] = games_df['weather'].apply(lambda x: x['condition'] if 'condition' in x.keys() else '')

        # Clean up dataframe
        games_df = games_df[games_df['gameDate'] <= datetime.now().replace(hour=23, minute=59, second=59)] # Don't try to get games way in future
        games_df.sort_values(by=['gamePk', 'gameDateTimeUTC'], ignore_index=True, inplace=True)
        return games_df[['gamePk', 'gameDateTimeUTC', 'awayTeamId', 'homeTeamId', 'awayScore', 'homeScore', 'awayStarterId', 'homeStarterId', 'awayLineup', 'homeLineup', 'stadiumId', 'dayGameFlag', 'weather', 'temperature', 'statcastFlag']]


    def get_atBats_from_mlb(self):
        # Load input event list and output event dictionary, if necessary
        input_events_set = len(self.input_events) > 0
        output_events_set = len(self.output_events.keys()) > 0
        if (not input_events_set) | (not output_events_set):
            event_types_df = self.read_collection('eventTypes')
            if not input_events_set:
                input_events = event_types_df[~event_types_df['inputEventType'].str.startswith('caught stealing')]['inputEventType'].tolist()
                input_events.append('')
                self.input_events = input_events
            if not output_events_set:
                self.output_events = dict(zip(event_types_df['outputEventType'], event_types_df['eventTypeId']))

        # Read csvs
        df = pd.concat([self.__read_statcast_csv(month=month) for month in range(4, 10)])
        column_names = ['gamePk', 'gameDateTimeUTC', 'atBatNumber', 'inning', 'inningBottomFlag', 'batterId', 'rightHandedBatterFlag', 'pitcherId', 'rightHandedPitcherFlag', 'xBA', 'eventTypeId']
        if len(df.index) == 0:
            return pd.DataFrame([], columns=column_names)

        # Calculated columns
        df['inningBottomFlag'] = df['inning_topbot'] == 'Bot'
        df['gameDate'] = df['game_date'].apply(lambda x: datetime.strptime(x, '%Y-%m-%d'))
        df['eventTypeId'] = df['events'].apply(lambda x: self.output_events[x])
        df['rightHandedBatterFlag'] = df['stand'] == 'R'
        df['rightHandedPitcherFlag'] = df['p_throws'] == 'R'
        games_df = self.read_collection('games', where_dict={'gameDateTimeUTC': {'$gte': datetime(self.date.year, 1, 1), '$lte': datetime(self.date.year, 12, 31)}})[['gamePk', 'gameDateTimeUTC']]
        games_df['gameDate'] = games_df['gameDateTimeUTC'].apply(lambda x: (x - timedelta(hours=5)).replace(hour=0, minute=0, second=0)) # This should help align with statcast dates
        df = pd.merge(df, games_df.rename({'gamePk': 'game_pk'}, axis=1), how='left', on=['game_pk', 'gameDate']) # Get actual datetime of game, not just date

        # Clean up dataframe
        df.rename({'game_pk': 'gamePk', 'at_bat_number': 'atBatNumber', 'batter': 'batterId', 'pitcher': 'pitcherId', 'estimated_ba_using_speedangle': 'xBA'}, inplace=True, axis=1)
        df.sort_values(['gamePk', 'gameDateTimeUTC', 'inning', 'atBatNumber'], ignore_index=True, inplace=True)
        return df[column_names]


    def __read_statcast_csv(self, month=4):
        year = self.date.year

        url_params = dict()
        url_params['all'] = 'true'
        url_params['hfAB'] = requests.utils.quote('|'.join(self.input_events).replace(' ', '\\.\\.'))
        url_params['hfGT'] = 'R%7C'
        url_params['hfSea'] = requests.utils.quote(f'{year}|')
        url_params['player_type'] = 'batter'
        if month < 5:
            url_params['game_date_lt'] = f'{year}-04-30'
        elif month > 8:
            url_params['game_date_gt'] = f'{year}-09-01'
        else:
            days_in_month = (dt(year, month + 1, 1) - timedelta(days=1)).day
            url_params['game_date_lt'] = f'{year}-0{month}-{days_in_month}'
            url_params['game_date_gt'] = f'{year}-0{month}-01'
        url_params['min_pitches'] = 0
        url_params['min_results'] = 0
        url_params['group_by'] = 'name'
        url_params['sort_col'] = 'pitches'
        url_params['player_event_sort'] = 'api_p_release_speed'
        url_params['sort_order'] = 'desc'
        url_params['min_pas'] = '0'
        url_params['type'] = 'details'

        url_params_string = '&'.join([f'{k}={v}' for k, v in url_params.items()]) + '&'
        url = f'https://baseballsavant.mlb.com/statcast_search/csv?{url_params_string}'
        df = pd.read_csv(url, usecols=['game_pk', 'game_date', 'inning_topbot', 'at_bat_number', 'batter', 'stand', 'pitcher', 'p_throws', 'estimated_ba_using_speedangle', 'inning', 'events'])
        return df
    ####################################
    ######## End Get From Web ##########
    ####################################


    ####################################
    ####### Get From Collection ########
    ####################################
    def read_collection(self, collection, where_dict={}):
        df = pd.DataFrame(self.read_collection_as_list(collection, where_dict=where_dict))
        for col in df.columns:
            if col.endswith('Lineup'):
                df[col] = df[col].apply(lambda x: tuple(x))
        return df


    def read_collection_as_list(self, collection, where_dict={}):
        return list(self.get_db()[collection].find(where_dict, {'_id': False}))


    def read_collection_to_html_table(self, collection, where_dict={}):
        return html_utils.html_table('dataView', self.read_collection_as_list(collection, where_dict=where_dict))
    ####################################
    ##### End Get From Collection ######
    ####################################


    ####################################
    ######## Update Collection #########
    ####################################
    def update_collection(self, collection, return_collection=False):
        return self.__update_db(collection, self.read_collection(collection), eval(f'self.get_{collection}_from_mlb()'), return_collection=return_collection)
    ####################################
    ###### End Update Collection #######
    ####################################


    ####################################
    ######### Clear Collection #########
    ####################################
    def clear_collection(self, collection, where_dict={}):
        deleted_records = self.get_db()[collection].delete_many(where_dict).deleted_count
        return f'Deleted {deleted_records} record(s) from {collection}.'
    ####################################
    ####### End Clear Collection #######
    ####################################


    ####################################
    ##### Add Column to Collection #####
    ####################################
    def add_column_to_collection(self, collection, column_name='', column_value=''):
        assert column_name, 'column_name argument must not be blank.'
        self.get_db()[collection].update_many({}, {'$set': {column_name: column_value}}, upsert=False, array_filters=None)
    ####################################
    ### End Add Column to Collection ###
    ####################################


    ####################################
    ### Rename Column in Collection ####
    ####################################
    def rename_column_in_collection(self, collection, old_name='', new_name=''):
        assert old_name in self.collection_columns(collection)['columns'], f'"{old_name}" is not a column in the "{collection}" collection.'
        assert new_name, 'new_name argument must not be blank.'
        self.get_db()[collection].update_many({}, {'$rename': {old_name: new_name}}, upsert=False, array_filters=None)
    ####################################
    # End Rename Column in Collection ##
    ####################################


    ####################################
    ### Drop Column from Collection ####
    ####################################
    def drop_column_from_collection(self, collection, column_name=''):
        assert column_name in self.collection_columns(collection)['columns'], f'"{column_name}" is not a column in the "{collection}" collection.'
        self.get_db()[collection].update_many({}, {'$unset': {column_name: ''}})
    ####################################
    # End Drop Column from Collection ##
    ####################################


    ####################################
    ###### Collection Information ######
    ####################################
    def collection_columns(self, collection):
        one_document, columns_with_info = self.get_db()[collection].find_one({}, {'_id': False}), dict()
        if one_document:
            columns = list(one_document.keys())
            for column in columns:
                column_value = one_document[column]
                columns_with_info[column] = type(column_value).__name__
        return {
            'collection': collection,
            'columns': columns_with_info
        }
    ####################################
    #### End Collection Information ####
    ####################################


    ####################################
    ######### Complex Queries ##########
    ####################################
    def get_available_dates(self, max_min=None):
        pipeline = [
            {
                '$match': {
                    'gameDateTimeUTC': {
                        '$lte': datetime.combine(self.date, datetime.max.time())
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
        result = [f'{date["_id"]["year"]}-{str(date["_id"]["month"]).zfill(2)}-{str(date["_id"]["day"]).zfill(2)}' for date in self.get_db()['games'].aggregate(pipeline)]
        return result[0] if return_one else result


    def stadium_park_factors(self, year=2022):
        return self.get_db()['parkFactors'].aggregate([
            {
                '$match': {
                    'year': year
                }
            }, {
                '$lookup': {
                    'from': 'stadiums',
                    'let': {
                        'stadiumId': '$stadiumId'
                    },
                    'pipeline': [
                        {
                            '$match': {
                                '$expr': {
                                    '$eq': [
                                        '$stadiumId',
                                        '$$stadiumId'
                                    ]
                                }
                            }
                        }
                    ],
                    'as': 'stadium'
                }
            }, {
                '$unwind': '$stadium'
            }, {
                '$group': {
                    '_id': {
                        'stadiumId': '$stadiumId'
                    },
                    'stadiumName': {
                        '$first': '$stadium.stadiumName'
                    },
                    'rightyDayParkFactor': {
                        '$max': {
                            '$cond': [
                                {
                                    '$and': [
                                        {
                                            '$eq': [
                                                '$dayGameFlag',
                                                True
                                            ]
                                        }, {
                                            '$eq': [
                                                '$rightHandedFlag',
                                                True
                                            ]
                                        }
                                    ]
                                },
                                '$parkFactor',
                                0
                            ]
                        }
                    },
                    'leftyDayParkFactor': {
                        '$max': {
                            '$cond': [
                                {
                                    '$and': [
                                        {
                                            '$eq': [
                                                '$dayGameFlag',
                                                True
                                            ]
                                        }, {
                                            '$eq': [
                                                '$rightHandedFlag',
                                                False
                                            ]
                                        }
                                    ]
                                },
                                '$parkFactor',
                                0
                            ]
                        }
                    },
                    'rightyNightParkFactor': {
                        '$max': {
                            '$cond': [
                                {
                                    '$and': [
                                        {
                                            '$eq': [
                                                '$dayGameFlag',
                                                False
                                            ]
                                        }, {
                                            '$eq': [
                                                '$rightHandedFlag',
                                                True
                                            ]
                                        }
                                    ]
                                },
                                '$parkFactor',
                                0
                            ]
                        }
                    },
                    'leftyNightParkFactor': {
                        '$max': {
                            '$cond': [
                                {
                                    '$and': [
                                        {
                                            '$eq': [
                                                '$dayGameFlag',
                                                False
                                            ]
                                        }, {
                                            '$eq': [
                                                '$rightHandedFlag',
                                                False
                                            ]
                                        }
                                    ]
                                },
                                '$parkFactor',
                                0
                            ]
                        }
                    }
                }
            }, {
                '$project': {
                    '_id': 0,
                    ' ': '<span><i class="fas fa-arrow-circle-right rowSelectorIcon"></i></span>',
                    'stadiumName': '$stadiumName',
                    'rightyDayParkFactor': '$rightyDayParkFactor',
                    'leftyDayParkFactor': '$leftyDayParkFactor',
                    'rightyNightParkFactor': '$rightyNightParkFactor',
                    'leftyNightParkFactor': '$leftyNightParkFactor'
                }
            }, {
                '$sort': {
                    'rightyNightParkFactor': -1
                }
            }
        ])


    def at_bat_span(self, where_dict={}):
        return self.get_db()['atBats'].aggregate([
            {
                '$match': where_dict
            }, {
                '$lookup': {
                    'from': 'games',
                    'let': {
                        'gamePk': '$gamePk',
                        'gameDateTimeUTC': '$gameDateTimeUTC'
                    },
                    'pipeline': [
                        {
                            '$match': {
                                '$expr': {
                                    '$and': [
                                        {
                                            '$eq': [
                                                '$gamePk',
                                                '$$gamePk'
                                            ]
                                        }, {
                                            '$eq': [
                                                '$gameDateTimeUTC',
                                                '$$gameDateTimeUTC'
                                            ]
                                        }
                                    ]
                                }
                            }
                        }
                    ],
                    'as': 'games'
                }
            }, {
                '$unwind': '$games'
            }, {
                '$lookup': {
                    'from': 'players',
                    'let': {
                        'pitcherId': '$pitcherId',
                        'year': {
                            '$year': '$gameDateTimeUTC'
                        }
                    },
                    'pipeline': [
                        {
                            '$match': {
                                '$expr': {
                                    '$and': [
                                        {
                                            '$eq': [
                                                '$year',
                                                '$$year'
                                            ]
                                        }, {
                                            '$eq': [
                                                '$playerId',
                                                '$$pitcherId'
                                            ]
                                        }
                                    ]
                                }
                            }
                        }
                    ],
                    'as': 'pitcherDetails'
                }
            }, {
                '$lookup': {
                    'from': 'players',
                    'let': {
                        'batterId': '$batterId',
                        'year': {
                            '$year': '$gameDateTimeUTC'
                        }
                    },
                    'pipeline': [
                        {
                            '$match': {
                                '$expr': {
                                    '$and': [
                                        {
                                            '$eq': [
                                                '$year',
                                                '$$year'
                                            ]
                                        }, {
                                            '$eq': [
                                                '$playerId',
                                                '$$batterId'
                                            ]
                                        }
                                    ]
                                }
                            }
                        }
                    ],
                    'as': 'batterDetails'
                }
            }, {
                '$lookup': {
                    'from': 'teams',
                    'let': {
                        'pitchingTeamId': {
                            '$cond': [
                                '$inningBottomFlag',
                                '$games.awayTeamId',
                                '$games.homeTeamId'
                            ]
                        },
                        'year': {
                            '$year': '$gameDateTimeUTC'
                        }
                    },
                    'pipeline': [
                        {
                            '$match': {
                                '$expr': {
                                    '$and': [
                                        {
                                            '$eq': [
                                                '$year',
                                                '$$year'
                                            ]
                                        }, {
                                            '$eq': [
                                                '$teamId',
                                                '$$pitchingTeamId'
                                            ]
                                        }
                                    ]
                                }
                            }
                        }
                    ],
                    'as': 'pitchingTeam'
                }
            }, {
                '$lookup': {
                    'from': 'teams',
                    'let': {
                        'battingTeamId': {
                            '$cond': [
                                '$inningBottomFlag',
                                '$games.homeTeamId',
                                '$games.awayTeamId'
                            ]
                        },
                        'year': {
                            '$year': '$gameDateTimeUTC'
                        }
                    },
                    'pipeline': [
                        {
                            '$match': {
                                '$expr': {
                                    '$and': [
                                        {
                                            '$eq': [
                                                '$year',
                                                '$$year'
                                            ]
                                        }, {
                                            '$eq': [
                                                '$teamId',
                                                '$$battingTeamId'
                                            ]
                                        }
                                    ]
                                }
                            }
                        }
                    ],
                    'as': 'battingTeam'
                }
            }, {
                '$lookup': {
                    'from': 'eventTypes',
                    'let': {
                        'eventTypeId': '$eventTypeId'
                    },
                    'pipeline': [
                        {
                            '$match': {
                                '$expr': {
                                    '$eq': [
                                        '$eventTypeId',
                                        '$$eventTypeId'
                                    ]
                                }
                            }
                        }
                    ],
                    'as': 'event'
                }
            }, {
                '$unwind': '$event'
            }, {
                '$unwind': '$pitcherDetails'
            }, {
                '$unwind': '$batterDetails'
            }, {
                '$unwind': '$pitchingTeam'
            }, {
                '$unwind': '$battingTeam'
            }, {
                '$sort': {
                    'gameDateTimeUTC': 1,
                    'gamePk': 1,
                    'atBatNumber': 1
                }
            }, {
                '$project': {
                    '_id': 0,
                    ' ': '<span><i class="fas fa-arrow-circle-right rowSelectorIcon"></i></span>',
                    'date': {
                        '$dateToString': {
                            'format': '%Y-%m-%d',
                            'date': '$gameDateTimeUTC',
                            'timezone': '-05:00'
                        }
                    },
                    'matchup': {
                        '$concat': [
                            {
                                '$cond': [
                                    '$inningBottomFlag',
                                    '$pitchingTeam.teamAbbreviation',
                                    '$battingTeam.teamAbbreviation'
                                ]
                            },
                            ' @ ',
                            {
                                '$cond': [
                                    '$inningBottomFlag',
                                    '$battingTeam.teamAbbreviation',
                                    '$pitchingTeam.teamAbbreviation'
                                ]
                            },
                            {
                                '$cond': [
                                    {
                                        '$gte': [
                                            '$games.awayScore',
                                            0
                                        ]
                                    },
                                    {
                                        '$concat': [
                                            ' (',
                                            {
                                                '$toString': '$games.awayScore'
                                            },
                                            ' - ',
                                            {
                                                '$toString': '$games.homeScore'
                                            },
                                            ')'
                                        ]
                                    },
                                    ''
                                ]
                            }
                        ]
                    },
                    'inning': {
                        '$concat': [
                            {
                                '$cond': [
                                    '$inningBottomFlag',
                                    'Bot. ',
                                    'Top '
                                ]
                            }, {
                                '$toString': '$inning'
                            }
                        ]
                    },
                    'pitcher': '$pitcherDetails.playerName',
                    'batter': '$batterDetails.playerName',
                    'lineupSpot': {
                        '$replaceAll': {
                            'input': {
                                '$toString': {
                                    '$add': [
                                        {
                                            '$indexOfArray': [
                                                {
                                                    '$cond': [
                                                        '$inningBottomFlag',
                                                        '$games.homeLineup',
                                                        '$games.awayLineup'
                                                    ]
                                                },
                                                '$batterId'
                                            ]
                                        },
                                        1
                                    ]
                                }
                            },
                            'find': '0',
                            'replacement': ''
                        }
                    },
                    'xBA': '$xBA',
                    'outcome': '$event.eventTypeName'
                }
            }
        ])


    def game_span(self, where_dict={}):
        return self.get_db()['games'].aggregate([
            {
                '$match': where_dict
            }, {
                '$lookup': {
                    'from': 'atBats',
                    'let': {
                        'gamePk': '$gamePk',
                        'gameDateTimeUTC': '$gameDateTimeUTC'
                    },
                    'pipeline': [
                        {
                            '$match': {
                                '$expr': {
                                    '$and': [
                                        {
                                            '$eq': [
                                                '$gamePk',
                                                '$$gamePk'
                                            ]
                                        }, {
                                            '$eq': [
                                                '$gameDateTimeUTC',
                                                '$$gameDateTimeUTC'
                                            ]
                                        }
                                    ]
                                }
                            }
                        }, {
                            '$group': {
                                '_id': {
                                    'gamePk': '$gamePk',
                                    'gameDateTimeUTC': '$gameDateTimeUTC'
                                },
                                'xH': {
                                    '$sum': {
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
                                },
                                'H': {
                                    '$sum': {
                                        '$cond': [
                                            {
                                                '$in': [
                                                    '$eventTypeId',
                                                    [event_type['eventTypeId'] for event_type in self.read_collection_as_list('eventTypes') if event_type['hitFlag']]
                                                ]
                                            },
                                            1,
                                            0
                                        ]
                                    }
                                }
                            }
                        }
                    ],
                    'as': 'atBats'
                }
            }, {
                '$lookup': {
                    'from': 'teams',
                    'let': {
                        'awayTeamId': '$awayTeamId',
                        'year': {
                            '$year': '$gameDateTimeUTC'
                        }
                    },
                    'pipeline': [
                        {
                            '$match': {
                                '$expr': {
                                    '$and': [
                                        {
                                            '$eq': [
                                                '$year',
                                                '$$year'
                                            ]
                                        }, {
                                            '$eq': [
                                                '$teamId',
                                                '$$awayTeamId'
                                            ]
                                        }
                                    ]
                                }
                            }
                        }
                    ],
                    'as': 'awayTeam'
                }
            }, {
                '$lookup': {
                    'from': 'teams',
                    'let': {
                        'homeTeamId': '$homeTeamId',
                        'year': {
                            '$year': '$gameDateTimeUTC'
                        }
                    },
                    'pipeline': [
                        {
                            '$match': {
                                '$expr': {
                                    '$and': [
                                        {
                                            '$eq': [
                                                '$year',
                                                '$$year'
                                            ]
                                        }, {
                                            '$eq': [
                                                '$teamId',
                                                '$$homeTeamId'
                                            ]
                                        }
                                    ]
                                }
                            }
                        }
                    ],
                    'as': 'homeTeam'
                }
            }, {
                '$lookup': {
                    'from': 'players',
                    'let': {
                        'awayStarterId': '$awayStarterId',
                        'year': {
                            '$year': '$gameDateTimeUTC'
                        }
                    },
                    'pipeline': [
                        {
                            '$match': {
                                '$expr': {
                                    '$and': [
                                        {
                                            '$eq': [
                                                '$year',
                                                '$$year'
                                            ]
                                        }, {
                                            '$eq': [
                                                '$playerId',
                                                '$$awayStarterId'
                                            ]
                                        }
                                    ]
                                }
                            }
                        }
                    ],
                    'as': 'awayStarter'
                }
            }, {
                '$lookup': {
                    'from': 'players',
                    'let': {
                        'homeStarterId': '$homeStarterId',
                        'year': {
                            '$year': '$gameDateTimeUTC'
                        }
                    },
                    'pipeline': [
                        {
                            '$match': {
                                '$expr': {
                                    '$and': [
                                        {
                                            '$eq': [
                                                '$year',
                                                '$$year'
                                            ]
                                        }, {
                                            '$eq': [
                                                '$playerId',
                                                '$$homeStarterId'
                                            ]
                                        }
                                    ]
                                }
                            }
                        }
                    ],
                    'as': 'homeStarter'
                }
            }, {
                '$unwind': '$awayTeam'
            }, {
                '$unwind': '$homeTeam'
            }, {
                '$unwind': {
                    'path': '$awayStarter',
                    'preserveNullAndEmptyArrays': True
                }
            }, {
                '$unwind': {
                    'path': '$homeStarter',
                    'preserveNullAndEmptyArrays': True
                }
            }, {
                '$unwind': {
                    'path': '$atBats',
                    'preserveNullAndEmptyArrays': True
                }
            }, {
                '$sort': {
                    'gameDateTimeUTC': 1
                }
            }, {
                '$project': {
                    '_id': 0,
                    ' ': '<span><i class="fas fa-arrow-circle-right rowSelectorIcon"></i></span>',
                    'date': {
                        '$dateToString': {
                            'format': '%Y-%m-%d',
                            'date': '$gameDateTimeUTC',
                            'timezone': '-05:00'
                        }
                    },
                    'time': {
                        '$dateToString': {
                            'format': '%H:%M',
                            'date': '$gameDateTimeUTC',
                            'timezone': '-05:00'
                        }
                    },
                    'matchup': {
                        '$concat': [
                            '$awayTeam.teamAbbreviation',
                            ' @ ',
                            '$homeTeam.teamAbbreviation',
                            {
                                '$cond': [
                                    {
                                        '$gte': [
                                            '$awayScore',
                                            0
                                        ]
                                    },
                                    {
                                        '$concat': [
                                            ' (',
                                            {
                                                '$toString': '$awayScore'
                                            },
                                            ' - ',
                                            {
                                                '$toString': '$homeScore'
                                            },
                                            ')'
                                        ]
                                    },
                                    ''
                                ]
                            }
                        ]
                    },
                    'awayStarter': '$awayStarter.playerName',
                    'homeStarter': '$homeStarter.playerName',
                    'weather': '$weather',
                    'temperature': {
                        '$concat': [
                            '$temperature',
                            u'\N{DEGREE SIGN}'
                        ]
                    },
                    'statcast?': {
                        '$cond': [
                            '$statcastFlag',
                            'Yes',
                            'No'
                        ]
                    },
                    'xH': {
                        '$toString': {
                            '$round': [
                                {
                                    '$ifNull': [
                                        '$atBats.xH',
                                        0
                                    ]
                                },
                                2
                            ]
                        }
                    },
                    'H': {
                        '$toString': {
                            '$round': [
                                {
                                    '$ifNull': [
                                        '$atBats.H',
                                        0
                                    ]
                                },
                                0
                            ]
                        }
                    }
                }
            }
        ])


    def eligible_batters(self, date=None):
        today = date.strftime('%Y-%m-%d') == (datetime.utcnow() - timedelta(hours=5)).strftime('%Y-%m-%d')
        return self.get_db()['players'].aggregate([
            {
                '$match': {
                    'year': date.year,
                    '$or': [
                        {
                            'position': {
                                '$ne': 'P'
                            }
                        }, {
                            'playerId': 660271 # Ohtani
                        }
                    ]
                }
            }, {
                '$lookup': {
                    'from': 'games',
                    'let': {
                        'teamId': '$teamId'
                    },
                    'pipeline': [
                        {
                            '$match': {
                                '$expr': {
                                    '$and': [
                                        {
                                            '$or': [
                                                {
                                                    '$eq': [
                                                        '$awayTeamId',
                                                        '$$teamId'
                                                    ]
                                                }, {
                                                    '$eq': [
                                                        '$homeTeamId',
                                                        '$$teamId'
                                                    ]
                                                }
                                            ]
                                        }, {
                                            '$gte': [
                                                '$gameDateTimeUTC',
                                                date + timedelta(hours=5)
                                            ]
                                        }, {
                                            '$lte': [
                                                '$gameDateTimeUTC',
                                                date + timedelta(hours=30)
                                            ]
                                        }
                                    ]
                                }
                            }
                        }
                    ],
                    'as': 'games'
                }
            }, {
                '$unwind': '$games'
            }, {
                '$lookup': {
                    'from': 'teams',
                    'let': {
                        'teamId': '$teamId'
                    },
                    'pipeline': [
                        {
                            '$match': {
                                '$expr': {
                                    '$and': [
                                        {
                                            '$eq': [
                                                '$teamId',
                                                '$$teamId'
                                            ]
                                        }, {
                                            '$eq': [
                                                '$year',
                                                date.year
                                            ]
                                        }
                                    ]
                                }
                            }
                        }
                    ],
                    'as': 'team'
                }
            }, {
                '$unwind': '$team'
            }, {
                '$project': {
                    '_id': 0,
                    'batter': '$playerId',
                    'name': {
                        '$cond': [
                            {
                                '$ne': [
                                    '$injuredFlag',
                                    True if today else None # health only matters if today
                                ]
                            },
                            '$playerName',
                            {
                                '$concat': [
                                    '<span>',
                                    '$playerName',
                                    '</span><span style="padding-left: 5px; color: red;"><i class="fas fa-kit-medical"></i></span>',
                                ]
                            }
                        ]  
                    },
                    'teamId': '$teamId',
                    'team': '$team.teamAbbreviation',
                    'bats': '$bats',
                    'gamePk': '$games.gamePk',
                    'time': {
                        '$dateToString': {
                            'format': '%H:%M',
                            'date': '$games.gameDateTimeUTC',
                            'timezone': '-05:00'
                        }
                    }
                }
            }
        ])


    def batter_span(self, where_dict={}):
        return self.get_db()['atBats'].aggregate([
            {
                '$match': where_dict
            }, {
                '$group': {
                    '_id': {
                        'gamePk': '$gamePk',
                        'gameDateTimeUTC': '$gameDateTimeUTC',
                        'batterId': '$batterId'
                    },
                    'gamePk': {
                        '$first': '$gamePk'
                    },
                    'gameDateTimeUTC': {
                        '$first': '$gameDateTimeUTC'
                    },
                    'batterId': {
                        '$first': '$batterId'
                    },
                    'xH': {
                        '$sum': {
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
                    },
                    'hits': {
                        '$sum': {
                            '$cond': [
                                {
                                    '$in': [
                                        '$eventTypeId',
                                        [event_type['eventTypeId'] for event_type in self.read_collection_as_list('eventTypes') if event_type['hitFlag']]
                                    ]
                                },
                                1,
                                0
                            ]
                        }
                    }
                }
            }, {
                '$group': {
                    '_id': {
                        'batterId': '$batterId'
                    },
                    'batterId': {
                        '$first': '$batterId'
                    },
                    'year': {
                        '$last': {
                            '$year': '$gameDateTimeUTC'
                        }
                    },
                    'G': {
                        '$sum': 1
                    },
                    'HG': {
                        '$sum': {
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
                    },
                    'xH / G': {
                        '$avg': '$xH'
                    }
                }
            }, {
                '$lookup': {
                    'from': 'players',
                    'let': {
                        'playerId': '$batterId',
                        'year': '$year'
                    },
                    'pipeline': [
                        {
                            '$match': {
                                '$expr': {
                                    '$and': [
                                        {
                                            '$eq': [
                                                '$playerId',
                                                '$$playerId'
                                            ]
                                        }, {
                                            '$eq': [
                                                '$year',
                                                '$$year'
                                            ]
                                        }
                                    ]
                                }
                            }
                        }
                    ],
                    'as': 'playerDetails'
                }
            }, {
                '$unwind': '$playerDetails'
            }, {
                '$sort': {
                    'xH / G': -1
                }
            }, {
                '$project': {
                    '_id': 0,
                    ' ': '<span><i class="fas fa-arrow-circle-right rowSelectorIcon"></i></span>',
                    'name': '$playerDetails.playerName',
                    'position': '$playerDetails.position',
                    'bats': '$playerDetails.bats',
                    'throws': '$playerDetails.throws',
                    'G': '$G',
                    'HG': '$HG',
                    'H %': {
                        '$toString': {
                            '$round': [
                                {
                                    '$multiply': [
                                        {
                                            '$divide': [
                                                '$HG',
                                                '$G'
                                            ]
                                        },
                                        100
                                    ]
                                },
                                2
                            ]
                        }
                    },
                    'xH / G': {
                        '$toString': {
                            '$round': [
                                '$xH / G',
                                2
                            ]
                        }
                    }
                }
            }
        ])


    def team_span(self, where_dict={}):
        return self.get_db()['atBats'].aggregate([
            {
                '$match': where_dict
            }, {
                '$group': {
                    '_id': {
                        'gamePk': '$gamePk',
                        'gameDateTimeUTC': '$gameDateTimeUTC',
                        'inningBottomFlag': '$inningBottomFlag'
                    },
                    'gamePk': {
                        '$first': '$gamePk'
                    },
                    'gameDateTimeUTC': {
                        '$first': '$gameDateTimeUTC'
                    },
                    'homeAway': {
                        '$first': {
                            '$cond': [
                                '$inningBottomFlag',
                                'home',
                                'away'
                            ]
                        }
                    },
                    'xBA': {
                        '$sum': {
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
                    },
                    'hits': {
                        '$sum': {
                            '$cond': [
                                {
                                    '$in': [
                                        '$eventTypeId',
                                        [event_type['eventTypeId'] for event_type in self.read_collection_as_list('eventTypes') if event_type['hitFlag']]
                                    ]
                                },
                                1,
                                0
                            ]
                        }
                    }
                }
            }, {
                '$lookup': {
                    'from': 'games',
                    'let': {
                        'atBatGamePk': '$gamePk',
                        'atBatGameDateTimeUTC': '$gameDateTimeUTC'
                    },
                    'pipeline': [
                        {
                            '$match': {
                                '$expr': {
                                    '$and': [
                                        {
                                            '$eq': [
                                                '$gamePk',
                                                '$$atBatGamePk'
                                            ]
                                        }, {
                                            '$eq': [
                                                '$gameDateTimeUTC',
                                                '$$atBatGameDateTimeUTC'
                                            ]
                                        }
                                    ]
                                }
                            }
                        }
                    ],
                    'as': 'gameDetails'
                }
            }, {
                '$unwind': '$gameDetails'
            }, {
                '$addFields': {
                    'battingTeamId': {
                        '$cond': [
                            {
                                '$eq': [
                                    '$homeAway',
                                    'home'
                                ]
                            },
                            '$gameDetails.homeTeamId',
                            '$gameDetails.awayTeamId'
                        ]
                    },
                    'pitchingTeamId': {
                        '$cond': [
                            {
                                '$eq': [
                                    '$homeAway',
                                    'home'
                                ]
                            },
                            '$gameDetails.awayTeamId',
                            '$gameDetails.homeTeamId'
                        ]
                    }
                }
            }, {
                '$project': {
                    'temp': [
                        {
                            'gamePk': '$gamePk',
                            'gameDateTimeUTC': '$gameDateTimeUTC',
                            'teamId': '$battingTeamId',
                            'side': 'B',
                            'xBA': '$xBA',
                            'hits': '$hits' 
                        }, {
                            'gamePk': '$gamePk',
                            'gameDateTimeUTC': '$gameDateTimeUTC',
                            'teamId': '$pitchingTeamId',
                            'side': 'P',
                            'xBA': '$xBA',
                            'hits': '$hits' 
                        }
                    ]
                }
            }, {
                '$unwind': '$temp'
            }, {
                '$replaceRoot': {
                    'newRoot': '$temp'
                }
            }, {
                '$group': {
                    '_id': {
                        'teamId': '$teamId',
                        'side': '$side'
                    },
                    'teamId': {
                        '$first': '$teamId'
                    },
                    'year': {
                        '$last': {
                            '$year': '$gameDateTimeUTC'
                        }
                    },
                    'side': {
                        '$first': '$side'
                    },
                    'xH': {
                        '$avg': '$xBA'
                    },
                    'H': {
                        '$avg': '$hits'
                    }
                }
            }, {
                '$group': {
                    '_id': {
                        'teamId': '$teamId'
                    },
                    'teamId': {
                        '$first': '$teamId'
                    },
                    'year': {
                        '$first': '$year'
                    },
                    'xH / G': {
                        '$max': {
                            '$cond': [
                                {
                                    '$eq': [
                                        '$side',
                                        'B'
                                    ]
                                },
                                '$xH',
                                0
                            ]
                        }
                    },
                    'H / G': {
                        '$max': {
                            '$cond': [
                                {
                                    '$eq': [
                                        '$side',
                                        'B'
                                    ]
                                },
                                '$H',
                                0
                            ]
                        }
                    },
                    'xHA / G': {
                        '$max': {
                            '$cond': [
                                {
                                    '$eq': [
                                        '$side',
                                        'P'
                                    ]
                                },
                                '$xH',
                                0
                            ]
                        }
                    },
                    'HA / G': {
                        '$max': {
                            '$cond': [
                                {
                                    '$eq': [
                                        '$side',
                                        'P'
                                    ]
                                },
                                '$H',
                                0
                            ]
                        }
                    }
                }
            }, {
                '$lookup': {
                    'from': 'teams',
                    'let': {
                        'teamId': '$teamId',
                        'year': '$year'
                    },
                    'pipeline': [
                        {
                            '$match': {
                                '$expr': {
                                    '$and': [
                                        {
                                            '$eq': [
                                                '$year',
                                                '$$year'
                                            ]
                                        }, {
                                            '$eq': [
                                                '$teamId',
                                                '$$teamId'
                                            ]
                                        }
                                    ]
                                }
                            }
                        }
                    ],
                    'as': 'team'
                }
            }, {
                '$unwind': '$team'
            }, {
                '$sort': {
                    'xH / G': -1
                }
            }, {
                '$project': {
                    '_id': 0,
                    ' ': '<span><i class="fas fa-arrow-circle-right rowSelectorIcon"></i></span>',
                    'abbr': '$team.teamAbbreviation',
                    'name': '$team.teamName',
                    'division': '$team.divisionName',
                    'xH / G': {
                        '$round': [
                            '$xH / G',
                            2
                        ]
                    },
                    'H / G': {
                        '$round': [
                            '$H / G',
                            2
                        ]
                    },
                    'xHA / G': {
                        '$round': [
                            '$xHA / G',
                            2
                        ]
                    },
                    'HA / G': {
                        '$round': [
                            '$HA / G',
                            2
                        ]
                    }
                }
            }
        ])
    ####################################
    ####### End Complex Queries ########
    ####################################


if __name__ == '__main__':
    pd.set_option('display.max_columns', None)
    pd.set_option('display.max_rows', None)
    pd.set_option('expand_frame_repr', False)
    if 'DATABASE_CLIENT' not in os.environ:
        os.environ['DATABASE_CLIENT'] = input('Database connection: ')
    date = dt(int(os.environ.get('YEAR')), 12, 31) if 'YEAR' in os.environ else None
    db = BTSHubMongoDB(os.environ.get('DATABASE_CLIENT'), 'bts-hub', date=date)

    update_type, update_confirmed, collections = sys.argv[1] if len(sys.argv) > 1 else None, 'Y', ['games']
    while update_type not in ['daily', 'hourly', 'clear']:
        update_type = input('Database update type must be either daily, hourly or clear. Which would you like to perform? ')

    if update_type in ['daily', 'clear']:
        collections = ['eventTypes', 'teams', 'players', 'atBats'] + collections + ['stadiums', 'parkFactors']
        if update_type == 'clear':
            update_confirmed = None
            while update_confirmed not in ['Y', 'N']:
                update_confirmed = input(f'Are you sure you want to clear out the following collections: {", ".join(collections)}? Y/N: ')

    if update_confirmed == 'Y':
        print(f'Updating database for the year {db.date.year}')
        for collection in collections:
            if update_type == 'clear':
                print(db.clear_collection(collection))
            elif update_type == 'hourly':
                db.update_collection(collection)
            else:
                db.update_collection(collection)
                time.sleep(5)