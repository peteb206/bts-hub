import os
import pymongo
from datetime import datetime
import requests
import pandas as pd
import json


class DataBase:
    def __init__(self, database_client, database_name, year=None):
        # Set up database connection
        self.__db = pymongo.MongoClient(database_client)[database_name]
        # Set up base urls
        self.__stats_api_url = 'https://statsapi.mlb.com/api/v1'
        self.__stats_api_url_ext = f'{self.__stats_api_url}/sports/1'
        self.year = year if year else datetime.now().year


    ####################################
    ############# Helpers ##############
    ####################################
    def get_db(self):
        return self.__db


    def read_from_db(self, collection, where_dict={}):
        return pd.DataFrame(list(self.get_db()[collection].find(where_dict, {'_id': False})))


    def __add_to_db(self, collection, records):
        assert (isinstance(records, pd.DataFrame)) | isinstance(records, list)
        if isinstance(records, list):
            pass
        elif isinstance(records, pd.DataFrame):
            records = records.to_dict('records')
        else:
            return None
        self.get_db()[collection].insert_many(records)
        return f'Added {len(records)} new records to {collection}.'


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
            combined = pd.merge(existing_df, new_df, how='outer', on=pks, suffixes=['_old', '_new'])
            combined['existing'] = combined.apply(lambda row: all([row[f'{col}_old'] == row[f'{col}_new'] for col in columns]), axis=1)
            combined = combined.drop([col for col in combined.columns if (col.endswith('_old')) | (col == 'existing')], axis=1).rename({f'{col}_new': col for col in columns}, axis=1)
            print(self.__update_records(collection, pks, columns, combined[~ combined['existing']]))
        return self.read_from_db(collection)


    def __update_records(self, collection, pks, columns, records):
        assert (isinstance(records, pd.DataFrame)) | isinstance(records, list)
        if isinstance(records, pd.DataFrame):
            records = records.to_dict('records')
        if isinstance(records, list):
            updated, added = 0, 0
            for record in records:
                update = self.__update_record(collection, pks, columns, record)
                if update.matched_count == 0:
                    added += 1
                    print(f'New record in {collection}:')
                else:
                    updated += 1
                    print(f'Updated record in {collection}:')
                print(record)
        return f'Added {added} new record(s) and updated {updated} existing record(s) in {collection}.'


    def __update_record(self, collection, pks, columns, record):
        return self.get_db()[collection].update_one(
            {
                pk: record[pk] for pk in pks
            },
            {
                '$set': {col: record[col] for col in columns}
            },
            upsert = True
        )


    def __get(self, url):
        request = requests.get(url)
        return json.loads(request.text)
    ####################################
    ########### End Helpers ############
    ####################################


    ####################################
    ####### Get From Collection ########
    ####################################
    ####################################
    ##### End Get From Collection ######
    ####################################


    ####################################
    ########## Get From Web ############
    ####################################
    def get_teams_from_mlb(self):
        teams_dict = self.__get(f'{self.__stats_api_url}/teams?lang=en&sportId=1&season={self.year}')
        teams_df = pd.DataFrame(teams_dict['teams'])[['season', 'id', 'name']]
        return teams_df.rename({'season': 'year', 'id': 'teamId', 'name': 'teamName'}, axis=1)
    ####################################
    ######## End Get From Web ##########
    ####################################


    ####################################
    ######## Update Collection #########
    ####################################
    def update_teams(self):
        return self.__update_db('teams', self.read_from_db('teams'), self.get_teams_from_mlb())
    ####################################
    ####### End Update Collection ########
    ####################################