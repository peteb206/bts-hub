from bs4 import BeautifulSoup
import pandas as pd
import numpy as np
import datetime
import time
import json
import re
from utils import stop_timer, game_time_func, utc_to_central
import requests


class ScrapeSession:
    def __init__(self):
        self.session = requests.Session()
        self.header = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/88.0.4324.182 Safari/537.36",
            "X-Requested-With": "XMLHttpRequest"
        }
        requests.packages.urllib3.disable_warnings(requests.packages.urllib3.exceptions.InsecureRequestWarning)


    def read_statcast_csv(self, home_away='home', inning=1, date=None):
        start_time = time.time() # Start timer

        start_date = date - datetime.timedelta(days=364) if date.month < 6 else datetime.date(date.year, 1, 1) # Bring in last year's data if April/May
        start_date_string = start_date.strftime('%Y-%m-%d')
        outcomes = [
            'strikeout',
            'field out',
            'single',
            'double',
            'triple',
            'home run',
            'strikeout double play',
            'walk',
            'double play',
            'field error',
            'grounded into double play',
            'fielders choice',
            'fielders choice out',
            'batter interference',
            'catcher interf',
            # 'caught stealing 2b',
            # 'caught stealing 3b',
            # 'caught stealing home',
            'force out',
            'hit by pitch',
            'intent walk',
            'sac bunt',
            'sac bunt double play',
            'sac fly',
            'sac fly double play',
            'triple play',
            ''
        ]
        years_string = f'{date.year}|' if date.year == start_date.year else f'{date.year}|{start_date.year}|'
        years_encoded = requests.utils.quote(years_string)
        outcomes_string = '|'.join(outcomes).replace(' ', '\\.\\.')
        outcomes_encoded = requests.utils.quote(outcomes_string)
        innings_string = f'{inning}|' if inning < 9 else '9|10|'
        innings_encoded = requests.utils.quote(innings_string)

        url_params = dict()
        url_params['all'] = 'true'
        url_params['hfAB'] = outcomes_encoded
        url_params['hfGT'] = 'R%7C'
        url_params['hfSea'] = years_encoded
        url_params['player_type'] = 'batter'
        url_params['game_date_gt'] = start_date_string
        url_params['home_road'] = home_away
        url_params['hfInn'] = innings_encoded
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
        df = pd.read_csv(url, usecols=['game_pk', 'game_date', 'away_team', 'home_team', 'at_bat_number', 'batter', 'stand', 'pitcher', 'p_throws', 'estimated_ba_using_speedangle', 'babip_value', 'events'])
        home_away_lower = 'home' if home_away == 'Home' else 'away'

        df['home_away'] = home_away_lower
        df['team'] = df[f'{home_away_lower}_team']
        df['opponent'] = df.apply(lambda row: row['away_team'] if home_away_lower == 'home' else row['home_team'], axis=1)
        df['hit'] = df['events'].apply(lambda event: 1 if event in ['home_run', 'triple', 'double', 'single'] else 0)

        stop_timer(f'\n{home_away} - inning {inning} - {len(df.index)} events', start_time) # Stop timer
        return df[['game_pk', 'game_date', 'team', 'opponent', 'home_away', 'at_bat_number', 'batter', 'stand', 'pitcher', 'p_throws', 'estimated_ba_using_speedangle', 'hit']]


    def get_player_info(self, year=2021, hitters=True, pitchers=True):
        start_time = time.time() # Start timer

        player_df1 = pd.read_json(f'https://statsapi.mlb.com/api/v1/sports/1/players?season={year}')
        player_df2 = pd.json_normalize(player_df1['people'])
        if hitters == False:
            player_df2 = player_df2[player_df2['primaryPosition.code'] == '1']
        if pitchers == False:
            player_df2 = player_df2[player_df2['primaryPosition.code'] != '1']
        player_df3 = player_df2.rename({'fullName': 'name', 'primaryPosition.abbreviation': 'position', 'batSide.code': 'B', 'pitchHand.code': 'T'}, axis=1)[['id', 'name', 'position', 'currentTeam.id', 'B', 'T']]

        teams_df = pd.read_json(f'https://statsapi.mlb.com/api/v1/teams?lang=en&sportId=1&season={year}')
        teams_id_map_df = pd.json_normalize(teams_df['teams'])[['id', 'abbreviation']].set_index('id')
        teams_id_map = teams_id_map_df['abbreviation'].to_dict()
        player_df3['team'] = player_df3['currentTeam.id'].apply(lambda teamId: teams_id_map[teamId])

        stop_timer('get_player_info()', start_time) # Stop timer
        return player_df3.drop('currentTeam.id', axis=1)


    def injured_player_ids(self, year=None):
        start_time = time.time() # Start timer

        load_date = self.session.get(f'https://www.fangraphs.com/api/roster-resource/injury-report/loaddate?season={year}', headers = self.header, verify = False).text.strip('\"')
        injuries = json.loads(self.session.get(f'https://cdn.fangraphs.com/api/roster-resource/injury-report/data?loaddate={load_date}&season={year}', headers = self.header).text)
        out = [injury['mlbamid'] for injury in injuries if injury['returndate'] == None]

        stop_timer('injured_player_ids()', start_time) # Stop timer
        return out


    def get_weather(self):
        start_time = time.time() # Start timer

        html = self.session.get('https://www.rotowire.com/baseball/weather.php', headers = self.header).text
        soup = BeautifulSoup(html, 'html.parser')

        teams = dict()
        for weather_box in soup.find_all('div', {'class': 'weather-box'}):
            weather_box_teams = weather_box.find('div', {'class': 'weather-box__teams'})
            weather_box_weather = weather_box.find('div', {'class': 'weather-box__weather'})
            for link in weather_box_teams.find_all('a'):
                teams[link['href'].split('=')[1]] = re.search('([^\/]+$)', weather_box_weather.find('img')['src']).group()
        if 'WAS' in teams.keys():
            teams['WSH'] = teams.pop('WAS')

        stop_timer('get_weather()', start_time) # Stop timer
        return teams


    def get_schedule(self, year=None, date=None, game_pk=None, lineups=False, is_today=True):
        start_time = time.time() # Start timer

        schedule_url = f'https://statsapi.mlb.com/api/v1/schedule?lang=en&sportId=1&gameType=R&'
        url_params = list()
        if year != None:
            url_params.append('season={}'.format(year))
        if date != None:
            url_params.append('date={}'.format(datetime.date.strftime(date, '%m/%d/%Y')))
        if game_pk != None:
            url_params.append('gamePk={}'.format(game_pk))
        if lineups == True:
            url_params.append('hydrate=probablePitcher,lineups,linescore')
        else:
            url_params.append('hydrate=probablePitcher')
        schedule_url += '&'.join(url_params)

        team_list = list()
        schedule = json.loads(requests.get(schedule_url).text)
        dates = schedule['dates']
        for date in dates:
            if year == None:
                year = date['date'].split('-')[0]
            games = date['games']
            for game in games:
                teams = game['teams']
                for home_away in ['away', 'home']:
                    team = teams[home_away]
                    team_dict = dict()
                    team_dict['game_pk'] = game['gamePk']
                    team_dict['game_date'] = date['date']
                    team_dict['game_time'] = game_time_func(game, is_today=is_today)
                    team_dict['id'] = team['team']['id']
                    team_dict['home_away'] = home_away

                    opposing_team = teams['home' if home_away == 'away' else 'away']
                    team_dict['opponent_id'] = opposing_team['team']['id']
                    team_dict['pitcher'] = opposing_team['probablePitcher']['id'] if 'probablePitcher' in opposing_team.keys() else 0
                    team_dict['lineup'] = list()
                    if 'lineups' in game.keys():
                        if f'{home_away}Players' in game['lineups'].keys():
                            team_dict['lineup'] = [player['id'] for player in game['lineups'][f'{home_away}Players']]
                    team_list.append(team_dict)
        out_df = pd.DataFrame(team_list)

        if year != None:
            teams_df1 = pd.read_json(f'https://statsapi.mlb.com/api/v1/teams?lang=en&sportId=1&season={year}')
            teams_id_map_df = pd.json_normalize(teams_df1['teams'])[['id', 'abbreviation']]
            out_df = pd.merge(out_df, teams_id_map_df, on='id').drop('id', axis=1).rename({'abbreviation': 'team', 'opponent_id': 'id'}, axis=1)
            out_df = pd.merge(out_df, teams_id_map_df, on='id').rename({'abbreviation': 'opponent'}, axis=1).drop('id', axis=1)

        stop_timer('get_schedule()', start_time) # Stop timer
        return out_df


    def batter_vs_pitcher(self):
        start_time = time.time() # Start timer

        html = self.session.get('https://baseballsavant.mlb.com/daily_matchups', headers = self.header).text
        data = re.search('(matchups_data\s*=\s*)(\[.*\])', html).group(2)
        df = pd.DataFrame(json.loads(data), columns = ['player_id', 'pitcher_id', 'pa', 'abs', 'hits', 'xba'])
        df['pa_vs_sp'] = df['pa'].fillna(0).astype(int, errors = 'ignore')
        df['abs'] = df['abs'].fillna(0).astype(int, errors = 'ignore')
        df['h_vs_sp'] = df['hits'].fillna(0).astype(int, errors = 'ignore')
        df['xba'] = df['xba'].fillna(np.nan).astype(float, errors = 'ignore')
        df['xH_vs_sp'] = round(df['xba'] * df['abs'], 2).fillna(0)

        stop_timer('batter_vs_pitcher()', start_time) # Stop timer
        return df.rename({'player_id': 'batter', 'pitcher_id': 'pitcher'}, axis=1)[['batter', 'pitcher', 'pa_vs_sp', 'h_vs_sp', 'xH_vs_sp']]