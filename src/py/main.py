import pandas as pd
import numpy as np
import requests
import json
import os
import datetime
import time
from scrape import Scrape
import calculate as calc
from utils import stop_timer, merge, int_columns


def load_table_data(dates=None, min_hits=None, is_today=False, from_app=False):
    start_time = time.time() # Start timer

    # This year
    if from_app == True:
        dates = dates.split(',')[0] # Only use the first date when reading from the app
    dates = dates.split(',')
    print('\n------------------------------', 'Calculating predictions for {}...'.format(', '.join(dates)), sep='\n')
    first_date = datetime.datetime.strptime(dates[0], '%Y-%m-%d').date()

    # Scrape data from Baseball Savant, MLB Stats API, Fangraphs and Rotowire
    scrape = Scrape()
    scraped_data_dict = scrape.get_data(date=first_date, is_today=is_today)
    player_info_df = scraped_data_dict['player_info']
    todays_games_df = scraped_data_dict['todays_games'] if 'todays_games' in scraped_data_dict.keys() else pd.DataFrame()
    statcast_year_df = calc.enrich_statcast_data(scraped_data_dict['statcast'])
    player_hits_by_game_df = statcast_year_df.groupby(['game_pk', 'batter'])['hit'].sum().reset_index()

    all_dfs, opponent_starter_df, opponent_bullpen_df, start_date, end_date = list(), None, None, None, None # Initialize some variables before iterating
    for date_string in dates:
        # Today
        print('--- NOTE: There are {} games on {}...'.format(len(todays_games_df.index), date_string))
        if len(todays_games_df.index) > 0:
            # There are games today
            statcast_df = statcast_year_df[statcast_year_df['game_date'] < date_string]
            game_date_values = statcast_df['game_date'].unique()
            start_date, end_date = game_date_values[0], game_date_values[-1]

            # Batting splits
            batter_per_game_df = calc.per_game_splits(statcast_df, index='batter')
            batter_per_game_home_away_splits_df = calc.per_game_splits(statcast_df, index=['batter', 'home_away'], suffix='_home_away')

            batter_per_pa_batters_vs_bullpen_df = calc.per_pa_splits(statcast_df[statcast_df['starter'] == False], index='batter', suffix='_vs_bullpen')
            batter_per_pa_batters_vs_p_throws_df = calc.per_pa_splits(statcast_df, index=['batter', 'p_throws'], suffix='_vs_p_throws')

            # Pitching splits
            pitcher_per_pa_df = calc.per_pa_splits(statcast_df, index='pitcher', suffix='_vs_all_b')
            pitcher_per_pa_vs_b_stands_df = calc.per_pa_splits(statcast_df, index=['pitcher', 'stand'], suffix='_vs_b_stand')
            bullpen_per_pa_df = calc.per_pa_splits(statcast_df[statcast_df['starter'] == False], index='opponent', suffix='_by_bullpen')

            # Eligible choices
            injured_player_ids = scraped_data_dict['injured_player_ids'] if 'injured_player_ids' in scraped_data_dict.keys() else list()
            healthy_batters_mask = (~player_info_df['id'].isin(injured_player_ids)) & ((player_info_df['position'] != 'P') | (player_info_df['name'] == 'Shohei Ohtani'))
            healthy_batters_df = player_info_df[healthy_batters_mask][['id', 'name', 'team', 'position', 'B']].rename({'id': 'batter'}, axis=1)
            pitcher_info_df = player_info_df[['id', 'T']].rename({'id': 'pitcher', 'T': 'p_throws'}, axis=1)

            # Combine data
            all_df = merge(
                todays_games_df,
                [
                    (healthy_batters_df, 'left', 'team', False),
                    (pitcher_per_pa_df, 'left', 'pitcher', False),
                    (pitcher_info_df, 'left', 'pitcher', False),
                    (batter_per_game_df, 'left', 'batter', False),
                    (batter_per_pa_batters_vs_bullpen_df, 'left', 'batter', False),
                    (batter_per_pa_batters_vs_p_throws_df, 'left', ['batter', 'p_throws'], False),
                    (batter_per_game_home_away_splits_df, 'left', ['batter', 'home_away'], False),
                    (bullpen_per_pa_df, 'left', 'opponent', False),
                    # (scraped_data_dict['head_to_head'], 'left', ['batter', 'pitcher'], False)
                ]
            )
            all_df['stand'] = np.where(all_df['B'] == 'S', np.where(all_df['p_throws'] == 'R', 'L', 'R'), all_df['B'])
            all_df = merge(
                all_df,
                [
                    (pitcher_per_pa_vs_b_stands_df, 'left', ['pitcher', 'stand'], False)
                ]
            )

            if from_app == True:
                all_df = calc.get_hit_probability(all_df) # add a predicted probability for getting 1+ hits
            else:
                all_df = pd.merge(all_df, player_hits_by_game_df, how='left', on=['game_pk', 'batter']) # include how many hits player actually got in game

            if len(dates) > 1:
                all_df['date'] = date_string

            all_dfs.append(all_df)

    all_df = pd.concat(all_dfs, ignore_index=True)
    all_df = int_columns(all_df, ['g', 'hit'])

    if from_app == False:
        out = all_df.fillna(0).round(4).to_dict(orient='records')
    else:
        out = {
            'rows': all_df[['game_pk', 'batter', 'probability', 'hit']].fillna(0).round(4).drop_duplicates().sort_values(by='probability', ascending=False).to_dict(orient='records'),
            'metrics': all_df.drop(['game_pk', 'probability', 'hit', 'order'], axis=1).drop_duplicates(subset='batter').set_index('batter').round(3).to_dict('index'),
            'opponents': {
                'starters': opponent_starter_df.fillna(0).round(3).to_dict(orient='index'),
                'bullpens': opponent_bullpen_df.fillna(0).round(3).to_dict(orient='index')
            },
            'games': todays_games_df.set_index('game_pk').to_dict(orient='index'),
            'headToHead': {},
            'weather': scraped_data_dict['weather'] if 'weather' in scraped_data_dict.keys() else dict(),
            'startDate': start_date,
            'endDate': end_date
        }

    stop_timer('Total', start_time) # Stop timer
    return out


def game_logs(player_type='batter', player_id=None, season=None):
    start_time = time.time() # Start timer

    if (player_type == None) | (player_id == None) | (season == None):
        out = {'error': 'type, year and batter/pitcher are required parameters'}
    else:
        url = 'https://baseballsavant.mlb.com/statcast_search?'
        url_params = [
            'hfGT=R%7C',
            'hfSea={}%7C'.format(season),
            'player_type={}'.format(player_type),
            'batters_lookup%5B%5D={}'.format(player_id),
            'min_pitches=0',
            'min_results=0',
            'group_by=name-date',
            'player_event_sort=api_p_release_speed',
            'min_pas=0',
            'chk_stats_pa=on',
            'chk_stats_abs=on',
            'chk_stats_bip=on',
            'chk_stats_hits=on',
            'chk_stats_k_percent=on',
            'chk_stats_bb_percent=on',
            'chk_stats_babip=on',
            'chk_stats_ba=on',
            'chk_stats_xba=on'
        ]
        url += '&'.join(url_params)
        df = pd.read_html(url)[0][['Date', 'PA', 'AB', 'BIP', 'Hits', 'xBA', 'BA', 'BABIP', 'K%', 'BB%']]
        df.sort_values(by='Date', ascending=False, ignore_index=True, inplace=True)
        for pct in ['K%', 'BB%']:
            df[pct] = df[pct].apply(lambda x: x / 100 if x > 0 else 0)
        df['Date'] = df['Date'].apply(lambda x: '-'.join(x.split('-')[1:]))

        out = {'data': df.round(4).to_dict(orient='records')}

    stop_timer('game_logs()', start_time) # Stop timer
    return out


def pick_history(year=None, date=None):
    start_time = time.time() # Start timer

    accounts = ['peteb206', 'pberryman6']
    session = requests.Session()
    header = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/88.0.4324.182 Safari/537.36",
        "X-Requested-With": "XMLHttpRequest"
    }

    picks = list()
    for account in accounts:
        okta_uid = os.environ.get('OKTA_UID_{}'.format(account.upper()))
        for url in [
            f'https://fantasy-lookup-service.mlb.com/fantasylookup/json/named.bts_profile_cmpsd.bam?bts_game_id=12&bts_user_recent_results.maxRows=300&timeframe=365&fntsy_game_id=10&bts_mulligan_status.game_id=bts{year}&okta_uid={okta_uid}',
            f'https://fantasy-lookup-service.mlb.com/fantasylookup/rawjson/named.bts_hitdd_picks.bam?ns=mlb&okta_uid={okta_uid}&max_days_back=0&max_days_ahead=0&bts_game_id=12&year={year}&focus_date={date}'
        ]:
            full_json = json.loads(session.get(url, headers = header).text)
            pick_history = full_json['bts_profile_cmpsd']['bts_user_recent_results']['queryResults']['row'] if 'bts_profile_cmpsd' in full_json.keys() else full_json['pick_dates'][0]['picks']
            pick_history_df = pd.DataFrame(pick_history)
            if len(pick_history_df.index) > 0:
                pick_history_df['game_date'] = pick_history_df['game_date'].apply(lambda x: x.split('T')[0])
                pick_history_df['opp_descriptor'] = pick_history_df['opp_descriptor'].apply(lambda x: x.split(',')[0])
                pick_history_df['account'] = account
                pick_history_df.rename({'name_display_first_last': 'name'}, axis = 1, inplace = True)
                picks.append(pick_history_df)

    picks_df = pd.concat(picks, ignore_index=True)[['ab', 'account', 'game_date', 'hit', 'name', 'opp_descriptor', 'player_id', 'status', 'streak']].fillna('').drop_duplicates(subset=['account', 'game_date', 'player_id'])
    out = {'data': picks_df.to_dict('records')}

    stop_timer('pick_history', start_time) # Stop timer
    return out


def main():
    # today = datetime.datetime.now().date().strftime('%Y-%m-%d')
    today = datetime.datetime(2018, 9, 2).strftime('%Y-%m-%d')
    return load_table_data(dates=today, min_hits=20, is_today=True, from_app=False)


if __name__ == '__main__':
    data = main()