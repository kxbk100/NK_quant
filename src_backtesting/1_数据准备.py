#!/usr/bin/python3
# -*- coding: utf-8 -*-

import os
import time
import pandas as pd
import numpy  as np
from glob   import glob
from joblib import Parallel, delayed

pd_display_rows  = 10
pd_display_cols  = 100
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
start_time = time.time()

from config import root_path, pickle_path


def transfer_raw_data_2_pkl_data(_trade_type='swap', _time_interval='5m', _njobs=16):
    # ====创建目录
    temp_path_ = os.path.join(pickle_path, f'{_trade_type}')
    if not os.path.exists(temp_path_):
        os.mkdir(temp_path_)

    trade_type_folder = _trade_type if '5m' == _time_interval else _trade_type + '_1m'
    path_list = glob(os.path.join(root_path, 'data', 'binance', trade_type_folder, '*', f'*_{_time_interval}.csv'))

    # ===整理每个币种对应的csv文件路径, 存储到dict中
    symbol_csv_data_dict = dict()
    for x in path_list:
        symbol  = os.path.splitext(os.path.basename(x))[0].replace('.csv', '').strip()
        symbol_ = symbol.replace(f'_{_time_interval}', '')
        
        if _trade_type == 'spot':
            # 过滤杠杆代币
            if symbol_ in ['COCOS-USDT', 'BTCST-USDT', 'DREP-USDT', 'SUN-USDT']:
                continue
            if symbol_.endswith(('DOWN-USDT', 'UP-USDT', 'BULL-USDT', 'BEAR-USDT')):
                continue
            if symbol_.endswith('USD'):
                continue
        #if symbol_!='BTC-USDT': continue

        if symbol not in symbol_csv_data_dict.keys():
            symbol_csv_data_dict[symbol] = set()
        symbol_csv_data_dict[symbol].add(x)


    results = dict()
    for symbol, path_list in symbol_csv_data_dict.items():
        print(_trade_type, symbol)
        symbol_ = symbol.replace(f'_{_time_interval}', '')
        # 并行读取 该币种所有对应周期的K线数据 并合并为一个 pd.DataFrame

        '''串行
        df_list = []
        for path_ in path_list:
            df_tmp = pd.read_csv(path_, header=1, encoding="GBK", parse_dates=['candle_begin_time'])
            df_list.append(df_tmp)
        df = pd.concat(df_list, ignore_index=True)
        '''

        df = pd.concat(Parallel(n_jobs=_njobs)(
            delayed(pd.read_csv)(path_, header=1, encoding="GBK", parse_dates=['candle_begin_time'])
            for path_ in path_list), ignore_index=True)
        df['symbol']    = symbol_.replace('-USDT', 'USDT').upper() # 增加 symbol
        df['avg_price'] = df['quote_volume'] / df['volume']     # 增加 均价

        df.sort_values(by='candle_begin_time', inplace=True)  # 排序
        df.drop_duplicates(subset=['candle_begin_time'], inplace=True, keep='last')  # 去除重复值
        df.reset_index(drop=True, inplace=True)  # 重置index
        results[symbol_] = df

    return results


def merge_alldata(results_5m, results_1m, trade_type):
    for symbol, df in results_5m.items():
        # =将数据转换为1小时周期
        df.set_index('candle_begin_time', inplace=True)
        df['avg_price_5m'] = df['avg_price']
        agg_dict = {
            'symbol': 'first',
            'open':   'first',
            'high':   'max',
            'low':    'min',
            'close':  'last',
            'volume': 'sum',
            'quote_volume': 'sum',
            'trade_num':    'sum',
            'taker_buy_base_asset_volume':  'sum',
            'taker_buy_quote_asset_volume': 'sum',
            'avg_price_5m': 'first'
        }
        df = df.resample(rule='1H').agg(agg_dict)

        # =针对1小时数据，补全空缺的数据。保证整张表没有空余数据
        df['symbol'].fillna(method='ffill',  inplace=True)
        # 对开、高、收、低、价格进行补全处理
        df['close'].fillna(method='ffill',   inplace=True)
        df['open'].fillna(value=df['close'], inplace=True)
        df['high'].fillna(value=df['close'], inplace=True)
        df['low'].fillna(value=df['close'],  inplace=True)
        # 将停盘时间的某些列，数据填补为0
        fill_0_list = ['volume', 'quote_volume', 'trade_num', 'taker_buy_base_asset_volume', 'taker_buy_quote_asset_volume']
        df.loc[:, fill_0_list] = df[fill_0_list].fillna(value=0)

        # 用1m均价代替1h open
        if symbol in results_1m.keys():
            df_1m = results_1m[symbol]
            df_1m.set_index('candle_begin_time', inplace=True)            
            df['avg_price_1m'] = df_1m['avg_price']

        # =计算最终的均价
        # 默认使用1分钟均价
        df['avg_price'] = df['avg_price_1m']
        # 没有1分钟均价就使用5分钟均价
        df['avg_price'].fillna(value=df['avg_price_5m'], inplace=True)
        # 没有5分钟均价就使用开盘价
        df['avg_price'].fillna(value=df['open'], inplace=True)
        del df['avg_price_5m']
        del df['avg_price_1m']

        df.reset_index(inplace=True)
        df.to_feather(os.path.join(pickle_path, f'{trade_type}', f'{symbol}.pkl'))


def check_avg(trade_type):
    print('\n')
    print('call check_avg---')
    path_list = glob(os.path.join(pickle_path, f'{trade_type}', '*.pkl'))
    for path_ in path_list:
        symbol = os.path.splitext(os.path.basename(path_))[0]
        df  = pd.read_feather(path_)
        df_ = df[df['avg_price'].isnull()]
        if not df_.empty:
            print('avg_price is null:', symbol, df_.shape[0])

        df_ = df[df['avg_price']==0]
        if not df_.empty:
            print('avg_price is zero:', symbol, df_.shape[0])


def run():
    for trade_type in ['spot', 'swap'][1:]:
        results_5m = transfer_raw_data_2_pkl_data(trade_type, '5m')
        results_1m = transfer_raw_data_2_pkl_data(trade_type, '1m')
        merge_alldata(results_5m, results_1m, trade_type)
        check_avg(trade_type)


if __name__ == '__main__':
    run()





















