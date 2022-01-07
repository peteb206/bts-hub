import pandas as pd
import numpy as np
import time
from utils import stop_timer, weighted_avg, weighted_sum
import statsmodels.api as sm


def enrich_statcast_data(df):
    start_time = time.time() # Start timer

    # Calculate batting order (Hold off and get this info from one big GET statsapi request in notebook or just from today's schedule if via the app)
    # df['order'] = df.groupby(['game_pk', 'home_away'])['batter'].cumcount() + 1
    # df['order'] = np.where(df['order'] < 10, df['order'], np.nan)

    # Calculate starting pitcher
    df = pd.merge(df, df.groupby(['game_date', 'game_pk', 'opponent'])['pitcher'].agg('first').reset_index().rename({'pitcher': 'starter'}, axis=1), on=['game_date', 'game_pk', 'opponent'])
    df['starter'] = (df['starter'] == df['pitcher'])

    # Calculate whether each game was tracked by statcast or not
    df['statcast'] = df.groupby('game_pk')['xBA'].transform('sum') > 0

    stop_timer('enrich_statcast_data()', start_time) # Stop timer
    return df


def per_game_splits(statcast_df, index=None, suffix=None):
    start_time = time.time() # Start timer

    agg_funcs_sum, agg_funcs_avg, weighted = ['sum'], ['mean'], False
    if (type(index) == type('')) | (len(index) == 1): # index is string or list of length 1
        if type(index) == type(''):
            index = [index]
        weighted = True
        agg_funcs_sum.append(weighted_sum)
        agg_funcs_avg.append(weighted_avg)

    df_by_game = statcast_df.groupby(['game_date', 'game_pk', 'statcast'] + index).agg({'hit': 'sum', 'xBA': 'sum'}).reset_index().rename({'xBA': 'xH'}, axis=1)
    df_by_game['H_1+'] = (df_by_game['hit'] >= 1)
    df_by_game['xH_1+'] = (df_by_game['xH'] >= 1)
    df_by_game['g'] = 1

    df_by_season = df_by_game.groupby(index).agg({'hit': agg_funcs_avg, 'xH': agg_funcs_avg, 'H_1+': agg_funcs_sum, 'xH_1+': agg_funcs_sum, 'statcast': 'sum', 'g': 'sum'}).rename({'hit': 'h_per_g', 'xH': 'xH_per_g'}, axis=1)
    if weighted == True:
        df_by_season.columns = [(f'{col[0]}*' if 'weighted' in col[1] else col[0]) for col in df_by_season.columns]
    else:
        df_by_season.columns = [col[0] for col in df_by_season.columns]

    df_by_season['hit_pct' if weighted == False else 'hit_pct*'] = df_by_season['H_1+'] / df_by_season['g']
    df_by_season['x_hit_pct' if weighted == False else 'x_hit_pct*'] = df_by_season['xH_1+'] / df_by_season['statcast']

    drop_cols = None
    if suffix != None:
        df_by_season.columns = [f'{col}{suffix}' for col in df_by_season.columns]
        drop_cols = [col for col in df_by_season.columns if ('H_1+' in col) | ('statcast' in col) | (col == f'g{suffix}')]
    else:
        drop_cols = [col for col in df_by_season.columns if ('H_1+' in col) | ('statcast' in col)]

    df_by_season = df_by_season.drop(drop_cols, axis=1).reset_index()

    stop_timer('per_game_splits()', start_time) # Stop timer
    return df_by_season


def per_pa_splits(statcast_df, index=None, suffix=None):
    start_time = time.time() # Start timer

    df = statcast_df.pivot_table(index=index, values=['hit', 'xBA']).reset_index()
    if type(index) == type(''):
        index = [index]
    df.columns = index + [f'h_per_pa{suffix}', f'xH_per_pa{suffix}']

    stop_timer('per_pa_splits()', start_time) # Stop timer
    return df


def get_hit_probability(df, opponent_starter_df, opponent_bullpen_df):
    start_time = time.time() # Start timer

    model = sm.load('log_reg_model.pickle')
    predictors = ['H_per_BF_vs_B_Hand', 'H_per_PA_vs_BP', 'H_per_PA_vs_SP_Hand', 'hit_bullpen', 'hit_pct_total', 'hit_pct_weighted', 'order', 'xBA_bullpen', 'xH_per_G_total', 'xH_per_G_weighted', 'x_hit_pct_total', 'x_hit_pct_weighted']
    df['probability'] = model.predict(df[predictors].astype(float))

    stop_timer('get_hit_probability()', start_time) # Stop timer
    return df