#!/usr/bin/python3
# -*- coding: utf-8 -*-

import os
import time
import numpy as np
import pandas as pd
from glob import glob
from datetime import datetime, timedelta
from joblib import Parallel, delayed
from utils  import diff,reader

pd_display_rows  = 20
pd_display_cols  = 8
pd_display_width = 1000
pd.set_option('display.max_rows', pd_display_rows)
pd.set_option('display.min_rows', pd_display_rows)
pd.set_option('display.max_columns', pd_display_cols)
pd.set_option('display.width', pd_display_width)
pd.set_option('display.max_colwidth', pd_display_width)
pd.set_option('display.unicode.ambiguous_as_wide', True)
pd.set_option('display.unicode.east_asian_width', True)
pd.set_option('expand_frame_repr', False)
os.environ['NUMEXPR_MAX_THREADS'] = "256"

from warnings import simplefilter
simplefilter(action='ignore', category=FutureWarning)

from config import pickle_path, data_path, head_columns
from config import filter_config_list


def cal_one_filter(df, filter_config):
    df = df.copy()
    class_name  = filter_config['filter'] 
    params_list = filter_config['params_list'] 
    """ ******************** 以下是需要修改的代码 ******************** """
    filter_list = []
    # =====技术指标
    _cls = __import__('filters.%s' % class_name,  fromlist=('', ))
    for params in params_list:
        str_params  = str(params)
        filter_name = f'{class_name}_fl_{str_params}'
        filter_list.append(filter_name)  
        # 计算因子
        getattr(_cls, 'signal')(df, params, filter_name)

    """ ************************************************************ """
    filter_list.sort()
    df = df[head_columns + filter_list]
    df.sort_values(by=['candle_begin_time', ], inplace=True)
    df.reset_index(drop=True, inplace=True)

    return df, class_name


def run(trade_type, njobs=16):
    print('\n')
    print(f'trade_type --- {trade_type}')
    # ===创建因子存储目录
    all_factor_path = os.path.join(data_path, trade_type)
    if not os.path.exists(all_factor_path):
        os.makedirs(all_factor_path)

    for pkl_file in glob(os.path.join(pickle_path, f'{trade_type}', '*USDT.pkl')):
        print('    ', pkl_file)
        symbol = os.path.basename(pkl_file).replace('.pkl', '')
        df = pd.read_feather(pkl_file)

        # =====处理原始数据
        df.sort_values(by='candle_begin_time', inplace=True)
        df.drop_duplicates(subset=['candle_begin_time'], inplace=True, keep='last') 
        df['下个周期_avg_price'] = df['avg_price'].shift(-1)  # 计算下根K线开盘买入涨跌幅

        '''如果需要 Funding Rate，请下载完数据，再打开注释   

        fundingrate_data = reader.read_fundingrate()
        df = pd.merge(df, 
            fundingrate_data[['candle_begin_time', 'symbol', 'fundingRate']], 
            on=['candle_begin_time', 'symbol'], how="left")

        df['fundingRate'].fillna(value=0, inplace=True)
        '''


        df.reset_index(drop=True, inplace=True)

        '''串行
        results = []
        for filter_config in filter_config_list:
            result = cal_one_filter(df, filter_config)
            results.append(result)
        '''

        results = Parallel(n_jobs=njobs)(
            delayed(cal_one_filter)(
                df, filter_config
            )
            for filter_config in filter_config_list
        )
        for df, class_name in results:
            symbol_factor_path = os.path.join(all_factor_path, symbol)
            if not os.path.exists(symbol_factor_path):
                os.makedirs(symbol_factor_path)
            # 保存文件头
            # 保存因子
            filter_path = os.path.join(symbol_factor_path, f'coin_alpha_filter_{class_name}.pkl')
            df_filters  = df[list(set(df.columns) - set(head_columns))]
            df_filters.to_feather(filter_path) 


if __name__ == '__main__':
    
    # ===计算因子
    for trade_type in ['spot', 'swap'][1:]:
        run(trade_type)












