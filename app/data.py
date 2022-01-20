import os
from socket import timeout
import sys
import pymongo
import requests
import pandas as pd
import json
from bs4 import BeautifulSoup
from requests_html import HTMLSession
from datetime import datetime, date as dt, timedelta
import time


class BTSHubMongoDB:
    def __init__(self, database_client, database_name, date=None):
        # Set up database connection
        self.__db = pymongo.MongoClient(database_client)[database_name]
        # Set up base urls
        self.__stats_api_url = 'https://statsapi.mlb.com/api/v1'
        self.__stats_api_url_ext = f'{self.__stats_api_url}/sports/1'
        self.__stats_api_default_params = 'lang=en&sportId=1'
        # Set dates
        today = dt.today()
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
        assert (isinstance(records, pd.DataFrame)) | isinstance(records, list)
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
            existing_df['_new'] = False
            new_df['_new'] = True
            combined_df = pd.concat([existing_df, new_df]).drop_duplicates(subset=pks + columns, keep=False, ignore_index=True)
            new_records_df= combined_df.drop_duplicates(subset=pks, keep=False).query('_new')
            if len(new_records_df.index) > 0:
                new_records_df, change = new_records_df[pks + columns], True
                print(self.__add_to_db(collection, new_records_df))
                print(new_records_df)
            updated_records_df = combined_df[combined_df['_new'] & combined_df.duplicated(subset=pks)].copy()
            if len(updated_records_df.index) > 0:
                updated_records_df, change = updated_records_df[pks + columns], True
                print(self.__update_records(collection, pks, columns, updated_records_df))
                print(updated_records_df)
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
        assert (isinstance(records, pd.DataFrame)) | isinstance(records, list)
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
                            'gameDate': '$gameDate'
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
        for attr in ['gamePk', 'gameDate']:
            df[attr] = df['_id'].apply(lambda x: x[attr])
        df.drop(['_id', 'xBA'], axis=1, inplace=True)
        df['statcastFlag'] = True
        return df


    def injured_player_ids(self):
        load_date = self.session.get(f'https://www.fangraphs.com/api/roster-resource/injury-report/loaddate?season={self.date.year}', headers = self.fangraphs_header).text.strip('\"')
        injuries = json.loads(self.session.get(f'https://cdn.fangraphs.com/api/roster-resource/injury-report/data?loaddate={load_date}&season={self.date.year}', headers = self.fangraphs_header).text)
        out = [injury['mlbamid'] for injury in injuries if injury['returndate'] == None]
        return out
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
                'inPlayFlag': event in ball_in_play_list,
                'hitFlag': event in hit_list
            })
            i += 1
        df = pd.DataFrame(events_list)

        # Clean up dataframe
        return df[['eventTypeId', 'inputEventType', 'outputEventType', 'inPlayFlag', 'hitFlag']]


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
                table = soup.find('table')
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
        teams_df = pd.DataFrame(teams_dict['teams'])[['season', 'id', 'abbreviation', 'name']]

        # Clean up dataframe
        teams_df.rename({'season': 'year', 'id': 'teamId', 'abbreviation': 'teamAbbreviation', 'name': 'teamName'}, axis=1, inplace=True)
        teams_df.sort_values(by=['year', 'teamId'], ignore_index=True, inplace=True)
        return teams_df[['year', 'teamId', 'teamAbbreviation', 'teamName']]


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


    def get_games_from_mlb(self):
        # Read json
        games_dict = self.__get(f'{self.__stats_api_url}/schedule?{self.__stats_api_default_params}&gameType=R&season={self.date.year}&hydrate=probablePitcher,lineups')

        # Calculated columns
        games_list = list()
        for date in games_dict['dates']:
            games_list += date['games']
        games_df = pd.DataFrame(games_list)[['gameDate', 'officialDate', 'gamePk', 'status', 'teams', 'lineups', 'venue', 'dayNight']]
        games_df['lineups'] = games_df['lineups'].apply(lambda x: x if isinstance(x, dict) else dict())
        for side in ['away', 'home']:
            games_df[f'{side}TeamId'] = games_df['teams'].apply(lambda x: x[side]['team']['id'])
            games_df[f'{side}StarterId'] = games_df['teams'].apply(lambda x: x[side]['probablePitcher']['id'] if 'probablePitcher' in x[side].keys() else 0)
            games_df[f'{side}Lineup'] = games_df['lineups'].apply(lambda x: tuple(player['id'] for player in x[f'{side}Players']) if f'{side}Players' in x.keys() else tuple())
        games_df['gameDateTimeUTC'] = games_df['gameDate'].apply(lambda x: datetime.strptime(x, '%Y-%m-%dT%H:%M:%SZ'))
        games_df['gameDate'] = games_df['gameDateTimeUTC'].apply(lambda x: (x - timedelta(hours=5)).replace(hour=0, minute=0, second=0)) # This should help align with statcast dates
        games_df['dayGameFlag'] = games_df['dayNight'] == 'day'
        games_df['stadiumId'] = games_df['venue'].apply(lambda x: x['id'])
        games_df['status'] = games_df['status'].apply(lambda x: x['statusCode'])
        games_df = pd.merge(games_df, self.get_statcast_games(), how='left', on=['gamePk', 'gameDate'])
        games_df['statcastFlag'].fillna(False, inplace=True)

        # Clean up dataframe
        games_df.sort_values(by=['gamePk', 'gameDateTimeUTC'], ignore_index=True, inplace=True)
        return games_df[['gamePk', 'gameDateTimeUTC', 'status', 'awayTeamId', 'homeTeamId', 'awayStarterId', 'homeStarterId', 'awayLineup', 'homeLineup', 'stadiumId', 'dayGameFlag', 'statcastFlag']]


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

        # Calculated columns
        df['inningBottomFlag'] = df['inning_topbot'] == 'Bot'
        df['gameDate'] = df['game_date'].apply(lambda x: datetime.strptime(x, '%Y-%m-%d'))
        df['eventTypeId'] = df['events'].apply(lambda x: self.output_events[x])
        df['rightHandedBatterFlag'] = df['stand'] == 'R'
        df['rightHandedPitcherFlag'] = df['p_throws'] == 'R'

        # Clean up dataframe
        df.rename({'game_pk': 'gamePk', 'at_bat_number': 'atBatNumber', 'batter': 'batterId', 'pitcher': 'pitcherId', 'estimated_ba_using_speedangle': 'xBA'}, inplace=True, axis=1)
        df.drop(['inning_topbot', 'game_date', 'stand', 'p_throws', 'events'], axis=1, inplace=True)
        df.sort_values(['gameDate', 'gamePk', 'inning', 'atBatNumber'], ignore_index=True, inplace=True)
        return df[['gamePk', 'gameDate', 'atBatNumber', 'inning', 'inningBottomFlag', 'batterId', 'rightHandedBatterFlag', 'pitcherId', 'rightHandedPitcherFlag', 'xBA', 'eventTypeId']]


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
        df = pd.DataFrame(list(self.get_db()[collection].find(where_dict, {'_id': False})))
        for col in df.columns:
            if col.endswith('Lineup'):
                df[col] = df[col].apply(lambda x: tuple(x))
        return df
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
        return self.get_db()[collection].update_many({}, {'$set': {column_name: column_value}}, upsert=False, array_filters=None)
    ####################################
    ### End Add Column to Collection ###
    ####################################


    ####################################
    ### Rename Column in Collection ####
    ####################################
    def rename_column_in_collection(self, collection, old_name='', new_name=''):
        return self.get_db()[collection].update_many({}, {'$rename': {old_name: new_name}}, upsert=False, array_filters=None)
    ####################################
    # End Rename Column in Collection ##
    ####################################


if __name__ == '__main__':
    pd.set_option('display.max_columns', None)
    pd.set_option('display.max_rows', None)
    pd.set_option('expand_frame_repr', False)
    if 'DATABASE_CONNECTION' not in os.environ:
        os.environ['DATABASE_CONNECTION'] = input('Database connection: ')
    db = BTSHubMongoDB(os.environ.get('DATABASE_CONNECTION'), 'bts-hub') # add date = dt(<year>, <month>, <day>) as necessary

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
        for collection in collections:
            if update_type == 'clear':
                print(db.clear_collection(collection))
            elif update_type == 'hourly':
                db.update_collection(collection)
            else:
                db.update_collection(collection)
                time.sleep(5)