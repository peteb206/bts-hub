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
from dateutil import tz


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


    def __update_db(self, collection, existing_df, new_df):
        if len(existing_df.columns) * len(existing_df.index) == 0: # Empty database collection
            print(self.__add_to_db(collection, new_df))
        else:
            # Find primary key(s) and column(s) of collection
            indices, pks = self.get_db()[collection].index_information(), list()
            for index, info in indices.items():
                if index != '_id_':
                    for pk_tup in info['key']:
                        pks.append(pk_tup[0])
            columns = [col for col in existing_df.columns if col not in pks]
            combined = pd.merge(existing_df, new_df, how='outer', on=pks, suffixes=['_old', '_new'], indicator=True)
            combined['existing'] = combined.apply(lambda row: (row['_merge'] == 'left_only') | all([row[f'{col}_old'] == row[f'{col}_new'] for col in columns]), axis=1) # Don't mess with rows with identical scraped row and rows where  
            diffs_df = combined[~combined['existing']]
            diffs_df = diffs_df.drop([col for col in diffs_df.columns if (col.endswith('_old')) | (col in ['_merge', 'existing'])], axis=1).rename({f'{col}_new': col for col in columns}, axis=1)
            print(self.__update_records(collection, pks, columns, diffs_df))
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
                    },
                    {
                        '$set': {col: record[col] for col in columns}
                    },
                    upsert = True
                )
                if update.matched_count == 0:
                    added += 1
                    # print(f'New record in {collection}:')
                else:
                    updated += 1
                    # print(f'Updated record in {collection}:')
                # print(record)
        return f'Added {added} new record(s) and updated {updated} existing record(s) in {collection}.'


    def __get(self, url):
        request = requests.get(url)
        return json.loads(request.text)
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
        players_df['currentTeam'] = players_df['currentTeam'].apply(lambda x: x['id'])
        players_df['primaryPosition'] = players_df['primaryPosition'].apply(lambda x: x['abbreviation'])
        players_df['batSide'] = players_df['batSide'].apply(lambda x: x['code'])
        players_df['pitchHand'] = players_df['pitchHand'].apply(lambda x: x['code'])
        injured_players_list = list() # TO DO

        # Clean up dataframe
        players_df['injuredFlag'] = players_df['id'].apply(lambda x: x in injured_players_list)
        players_df.rename({'id': 'playerId', 'fullName': 'playerName', 'currentTeam': 'teamId', 'primaryPosition': 'position', 'batSide': 'bats', 'pitchHand': 'throws'}, axis=1, inplace=True)
        players_df.sort_values(by=['year', 'teamId', 'playerId'], ignore_index=True, inplace=True)
        return players_df[['year', 'playerId', 'teamId', 'playerName', 'position', 'bats', 'throws', 'injuredFlag']]


    def get_games_from_mlb(self):
        # Read json
        games_dict = self.__get(f'{self.__stats_api_url}/schedule?{self.__stats_api_default_params}&gameType=R&season={self.date.year}&hydrate=probablePitcher,lineups')

        # Calculated columns
        games_list = list()
        for date in games_dict['dates']:
            games_list += date['games']
        games_df = pd.DataFrame(games_list)[['gameDate', 'officialDate', 'gamePk', 'teams', 'lineups', 'venue', 'dayNight']]
        for side in ['away', 'home']:
            games_df[f'{side}TeamId'] = games_df['teams'].apply(lambda x: x[side]['team']['id'])
            games_df[f'{side}StarterId'] = games_df['teams'].apply(lambda x: x[side]['probablePitcher']['id'] if 'probablePitcher' in x[side].keys() else 0)
            games_df[f'{side}Lineup'] = games_df['lineups'].apply(lambda x: [player['id'] for player in x[f'{side}Players']] if isinstance(x, dict) else list())
        games_df['gameDateTimeUTC'] = games_df['gameDate'].apply(lambda x: datetime.strptime(x, '%Y-%m-%dT%H:%M:%SZ').replace(tzinfo=tz.gettz('UTC')))
        games_df['dayGameFlag'] = games_df['dayNight'] == 'day'
        games_df['stadiumId'] = games_df['venue'].apply(lambda x: x['id'])
        games_df['statcastFlag'] = True # TO DO Merge with atBats data to see if statcast data was collected

        # Clean up dataframe
        games_df.sort_values(by=['gamePk', 'gameDateTimeUTC'], ignore_index=True, inplace=True)
        return games_df[['gamePk', 'gameDateTimeUTC', 'awayTeamId', 'homeTeamId', 'awayStarterId', 'homeStarterId', 'awayLineup', 'homeLineup', 'stadiumId', 'dayGameFlag', 'statcastFlag']]


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
        return df[['gamePk', 'gameDate', 'inning', 'atBatNumber', 'batterId', 'rightHandedBatterFlag', 'pitcherId', 'rightHandedPitcherFlag', 'xBA', 'eventTypeId']]


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
        return pd.DataFrame(list(self.get_db()[collection].find(where_dict, {'_id': False})))
    ####################################
    ##### End Get From Collection ######
    ####################################


    ####################################
    ######## Update Collection #########
    ####################################
    def update_collection(self, collection):
        return self.__update_db(collection, self.read_collection(collection), eval(f'self.get_{collection}_from_mlb()'))
    ####################################
    ###### End Update Collection #######
    ####################################


    ####################################
    ######### Clear Collection #########
    ####################################
    def clear_collection(self, collection):
        deleted_records = self.get_db()[collection].delete_many({}).deleted_count
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
    if 'DATABASE_CONNECTION' not in os.environ:
        os.environ['DATABASE_CONNECTION'] = input('Database connection: ')
    db = BTSHubMongoDB(os.environ.get('DATABASE_CONNECTION'), 'bts-hub', date=datetime.now().date()) # sub with dt(<year>, <month>, <day>) as necessary
    update_type = sys.argv[1] if len(sys.argv) > 1 else os.environ.get('UPDATE_TYPE') if 'UPDATE_TYPE' in os.environ else None
    while update_type not in ['daily', 'hourly', 'clear']:
        update_type = input('Database update type must be either daily, hourly or clear. Which would you like to perform? ')

    collections = ['eventTypes', 'teams', 'players', 'atBats', 'games', 'stadiums', 'parkFactors'] if update_type in ['daily', 'clear'] else ['games']
    update_confirmed = None if update_type == 'clear' else 'Y'
    while update_confirmed not in ['Y', 'N']:
        update_confirmed = input(f'Are you sure you want to clear out the following collections: {", ".join(collections)}? Y/N: ')
    if update_confirmed == 'Y':
        for collection in collections:
            if update_type == 'clear':
                print(db.clear_collection(collection))
            else:
                print(collection, db.update_collection(collection), sep='\n')