#!/usr/bin/python3
# -*- coding: utf-8 -*-

import os
import pandas as pd
import numpy  as np
from glob   import glob
from joblib import Parallel, delayed

from config import pickle_path, data_path, fundingrate_path


def get_factors_path(trade_type, factor_class_list):
    result = dict()

    for path in glob(os.path.join(data_path, trade_type, '*', 'coin_alpha_factor_*.pkl')):
        symbol = path.split(os.sep)[-2]
        class_name = path.split(os.sep)[-1].replace('.pkl', '').replace('coin_alpha_factor_', '')
        if class_name in factor_class_list:
            if symbol not in result.keys():
                result[symbol] = []
            result[symbol].append(path)  

    return result  


def read_one(trade_type, hold_hour, symbol, path_list, filter_class_list, date_range=()):
    df = pd.read_feather(os.path.join(data_path, trade_type, symbol, 'coin_alpha_head.pkl'))

    # 读因子文件
    feature_list = []
    for path in path_list:
        df_ = pd.read_feather(path)
        for f in df_.columns:
            df[f] = df_[f]
            feature_list.append(f)
    # 读过滤文件
    filter_list = []
    for filter_name in filter_class_list:
        filter_path = os.path.join(data_path, trade_type, symbol, f'coin_alpha_filter_{filter_name}.pkl')
        df_ = pd.read_feather(filter_path)
        filter_columns = list(set(df_.columns))
        for f in filter_columns:
            df[f] = df_[f]
            filter_list.append(f)

    df.sort_values(by=['candle_begin_time', ], inplace=True)
    df.reset_index(drop=True, inplace=True)
    #"""
    # 删除前N行
    df = df.iloc[999:]  
    # 处理极端情况
    if df.empty: return
    df.reset_index(drop=True, inplace=True)
    #"""

    # ========以上是需要修改的代码
    #df = df.loc[0:0, :].append(df, ignore_index=True)
    #df.loc[0, 'candle_begin_time'] = pd.to_datetime('2010-01-01')
    # ===将数据转化为需要的周期
    df.set_index('candle_begin_time', inplace=True)
    # 必备字段
    agg_dict = {
        'symbol': 'first',
        'avg_price': 'first',
        '下个周期_avg_price': 'last',
        'volume': 'sum',
    }
    for f in feature_list + filter_list:
        agg_dict[f] = 'first' 

    period_df_list = []
    for offset in range(int(hold_hour[:-1])):
        # period_df = df.resample(hold_hour, offset=offset).agg(agg_dict)
        period_df = df.resample(hold_hour, offset=f'{offset}H').agg(agg_dict)
        period_df.reset_index(inplace=True)
        # 合并数据
        period_df_list.append(period_df)

    # 将不同offset的数据，合并到一张表
    period_df = pd.concat(period_df_list, ignore_index=True)
    period_df.sort_values(by='candle_begin_time', inplace=True)

    if len(date_range) == 0:
        period_df = period_df[period_df['candle_begin_time'] >= pd.to_datetime('2018-01-01')]  # 删除2018年之前的数据
    else:
        start_date, end_date = date_range
        period_df = period_df[period_df['candle_begin_time'] >= pd.to_datetime(start_date)]
        period_df = period_df[period_df['candle_begin_time'] <= pd.to_datetime(end_date)]

    # 数据中间空缺比较多, resample之后可能会填充symbol=nan的数据
    period_df.dropna(subset=['symbol'], inplace=True)
    period_df.reset_index(drop=True, inplace=True)

    return period_df
    

def readall(trade_type, hold_hour, factor_class_list, filter_class_list=[], njobs=16, date_range=()):
    factor_paths = get_factors_path(trade_type, factor_class_list)

    '''串行
    all_list = []
    for symbol, path_list in factor_paths.items():
        result = read_one(trade_type, hold_hour, symbol, path_list, filter_class_list, date_range)
        all_list.append(result)
    '''

    # '''
    all_list = Parallel(n_jobs=njobs)(
        delayed(read_one)(trade_type, hold_hour, symbol, path_list, filter_class_list, date_range)
            for symbol, path_list in factor_paths.items()
    )

    all_df = pd.concat(all_list, ignore_index=True)
    all_df.sort_values(by=['candle_begin_time', 'symbol'], inplace=True)
    all_df.reset_index(drop=True, inplace=True)

    return all_df



def readhour(trade_type, factor_class_list, filter_class_list=[], njobs=16):
    def _read(trade_type, symbol, path_list, filter_class_list):
        df = pd.read_feather(os.path.join(data_path, trade_type, symbol, 'coin_alpha_head.pkl'))

        # 读因子文件
        feature_list = []
        for path in path_list:
            df_ = pd.read_feather(path)
            for f in df_.columns:
                df[f] = df_[f]
                feature_list.append(f)
        # 读过滤文件
        filter_list = []
        for filter_name in filter_class_list:
            filter_path = os.path.join(data_path, trade_type, symbol, f'coin_alpha_filter_{filter_name}.pkl')
            df_ = pd.read_feather(filter_path)
            filter_columns = list(set(df_.columns))
            for f in filter_columns:
                df[f] = df_[f]
                filter_list.append(f)

        df.sort_values(by=['candle_begin_time', ], inplace=True)
        df.reset_index(drop=True, inplace=True)
        # 删除前N行
        #df.drop(df.index[:999], inplace=True)
        df = df.iloc[999:]  
        # 处理极端情况
        if df.empty: return
        df.reset_index(drop=True, inplace=True)
        return df

    factor_paths = get_factors_path(trade_type, factor_class_list)

    all_list = Parallel(n_jobs=njobs)(
        delayed(_read)(trade_type, symbol, path_list, filter_class_list)
            for symbol, path_list in factor_paths.items()
    )

    all_df = pd.concat(all_list, ignore_index=True)
    all_df.sort_values(by=['candle_begin_time', 'symbol'], inplace=True)
    all_df.reset_index(drop=True, inplace=True)

    return all_df


def readori(trade_type, njobs=16):
    def _read(pkl_file):
        df = pd.read_feather(pkl_file)
        df.sort_values(by='candle_begin_time', inplace=True)
        df.drop_duplicates(subset=['candle_begin_time'], inplace=True, keep='last') 
        df.reset_index(drop=True, inplace=True)
        return df

    all_list = Parallel(n_jobs=njobs)(
        delayed(_read)(pkl_file)
            for pkl_file in glob(os.path.join(pickle_path, f'{trade_type}', '*USDT.pkl'))
    )
    all_df = pd.concat(all_list, ignore_index=True)
    all_df.sort_values(by=['candle_begin_time', 'symbol'], inplace=True)
    all_df.reset_index(drop=True, inplace=True)
    return all_df  


def read_fundingrate(njobs=16):
    def _read(pkl_file):
        df = pd.read_feather(pkl_file)
        df.sort_values(by='candle_begin_time', inplace=True)
        df.drop_duplicates(subset=['candle_begin_time'], inplace=True, keep='last') 
        df.reset_index(drop=True, inplace=True)
        return df

    all_list = Parallel(n_jobs=njobs)(
        delayed(_read)(pkl_file)
            for pkl_file in glob(os.path.join(fundingrate_path, '*USDT.pkl'))
    )
    all_df = pd.concat(all_list, ignore_index=True)
    all_df.sort_values(by=['candle_begin_time', 'symbol'], inplace=True)
    all_df.reset_index(drop=True, inplace=True)
    return all_df

def feature_shift(df, feature_list, num=1):
    # 根据回测时对齐下周期收益的逻辑，按需shift
    df[feature_list] = df.groupby('symbol')[feature_list].shift(num)
    return df

    


