#!/usr/bin/python3
# -*- coding: utf-8 -*-

import datetime
import os
import time
from concurrent.futures import ThreadPoolExecutor
from glob import glob
from multiprocessing import cpu_count

import pandas as pd
from joblib import Parallel, delayed
from config import root_path, pickle_path,src_path
import sys
sys.path.append(src_path)

from data_center.download_util import get_local_path

pd_display_rows = 10
pd_display_cols = 100
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
        symbol = os.path.splitext(os.path.basename(x))[0].replace('.csv', '').strip()
        symbol_ = symbol.replace(f'_{_time_interval}', '')

        if _trade_type == 'spot':
            # 过滤杠杆代币
            if symbol_ in ['COCOS-USDT', 'BTCST-USDT', 'DREP-USDT', 'SUN-USDT']:
                continue
            if symbol_.endswith(('DOWN-USDT', 'UP-USDT', 'BULL-USDT', 'BEAR-USDT')):
                continue
            if symbol_.endswith('USD'):
                continue
        # if symbol_!='BTC-USDT': continue

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
        df['symbol'] = symbol_.replace('-USDT', 'USDT').upper()  # 增加 symbol
        df['avg_price'] = df['quote_volume'] / df['volume']  # 增加 均价

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
            'open': 'first',
            'high': 'max',
            'low': 'min',
            'close': 'last',
            'volume': 'sum',
            'quote_volume': 'sum',
            'trade_num': 'sum',
            'taker_buy_base_asset_volume': 'sum',
            'taker_buy_quote_asset_volume': 'sum',
            'avg_price_5m': 'first'
        }
        df = df.resample(rule='1H').agg(agg_dict)

        # =针对1小时数据，补全空缺的数据。保证整张表没有空余数据
        df['symbol'].fillna(method='ffill', inplace=True)
        # 对开、高、收、低、价格进行补全处理
        df['close'].fillna(method='ffill', inplace=True)
        df['open'].fillna(value=df['close'], inplace=True)
        df['high'].fillna(value=df['close'], inplace=True)
        df['low'].fillna(value=df['close'], inplace=True)
        # 将停盘时间的某些列，数据填补为0
        fill_0_list = ['volume', 'quote_volume', 'trade_num', 'taker_buy_base_asset_volume',
                       'taker_buy_quote_asset_volume']
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
        df = pd.read_feather(path_)
        df_ = df[df['avg_price'].isnull()]
        if not df_.empty:
            print('avg_price is null:', symbol, df_.shape[0])
            print(df_[5])

        df_ = df[df['avg_price'] == 0]
        if not df_.empty:
            print('avg_price is zero:', symbol, df_.shape[0])


def read_csv_resample_2_pkl(_trade_type, symbol_, path_list, path_list_1m, _njobs=16):
    '''
    读取csv文件列表,并resample为1H周期,最后写入pkl
    '''

    '''串行
    df_list = []
        for path_ in path_list:
            df_tmp = pd.read_csv(path_, header=1, encoding="GBK", parse_dates=['candle_begin_time'])
            df_list.append(df_tmp)
        df = pd.concat(df_list, ignore_index=True)
    '''
    # 并行读取 该币种所有对应周期的K线数据 并合并为一个 pd.DataFrame
    df = pd.concat(Parallel(_njobs)(
        delayed(pd.read_csv)(path_, header=1, encoding="GBK", parse_dates=['candle_begin_time'])
        for path_ in path_list), ignore_index=True)

    df['symbol'] = symbol_.replace('-USDT', 'USDT').upper()  # 增加 symbol
    df['avg_price_5m'] = df['quote_volume'] / df['volume']  # 增加 均价

    df.sort_values(by='candle_begin_time', inplace=True)  # 排序
    df.drop_duplicates(subset=['candle_begin_time'], inplace=True, keep='last')  # 去除重复值
    df.reset_index(drop=True, inplace=True)  # 重置index
    df.set_index('candle_begin_time', inplace=True)
    agg_dict = {
        'symbol': 'first',
        'open': 'first',
        'high': 'max',
        'low': 'min',
        'close': 'last',
        'volume': 'sum',
        'quote_volume': 'sum',
        'trade_num': 'sum',
        'taker_buy_base_asset_volume': 'sum',
        'taker_buy_quote_asset_volume': 'sum',
        'avg_price_5m': 'first'
    }
    # =将数据转换为1小时周期
    df = df.resample(rule='1H').agg(agg_dict)

    # =针对1小时数据，补全空缺的数据。保证整张表没有空余数据
    df['symbol'].fillna(method='ffill', inplace=True)
    # 对开、高、收、低、价格进行补全处理
    df['close'].fillna(method='ffill', inplace=True)
    df['open'].fillna(value=df['close'], inplace=True)
    df['high'].fillna(value=df['close'], inplace=True)
    df['low'].fillna(value=df['close'], inplace=True)
    # 将停盘时间的某些列，数据填补为0
    fill_0_list = ['volume', 'quote_volume', 'trade_num', 'taker_buy_base_asset_volume',
                   'taker_buy_quote_asset_volume']
    df.loc[:, fill_0_list] = df[fill_0_list].fillna(value=0)

    # 读取1分钟数据
    df_1m = pd.concat(Parallel(_njobs)(
        delayed(pd.read_csv)(path_, header=1, encoding="GBK", parse_dates=['candle_begin_time'])
        for path_ in path_list_1m), ignore_index=True)
    df_1m['avg_price'] = df_1m['quote_volume'] / df_1m['volume']
    df_1m.set_index('candle_begin_time', inplace=True)
    df['avg_price_1m'] = df_1m['avg_price']

    # =计算最终的均价
    # 默认使用1分钟均价
    df['avg_price'] = df['avg_price_1m']
    # 没有1分钟均价就使用5分钟均价
    df['avg_price'].fillna(value=df['avg_price_5m'], inplace=True)
    # 没有5分钟均价就使用开盘价
    df['avg_price'].fillna(value=df['open'], inplace=True)

    df.reset_index(inplace=True)
    df.to_feather(os.path.join(pickle_path, f'{_trade_type}', f'{symbol_}.pkl'))
    print(symbol_, '处理完毕')


def csv_2_pkl(_trade_type, _time_interval='5m', _njobs=16):
    # ====创建目录
    temp_path_ = os.path.join(pickle_path, f'{_trade_type}')
    if not os.path.exists(temp_path_):
        os.mkdir(temp_path_)

    trade_type_folder_5m = _trade_type
    trade_type_folder_1m = _trade_type + '_1m'

    # ===整理每个币种对应的csv文件路径, 存储到symbol_csv_data_dict中
    symbol_csv_data_dict_5m = glob_csv_dict(_trade_type, trade_type_folder_5m, '5m')
    symbol_csv_data_dict_1m = glob_csv_dict(_trade_type, trade_type_folder_1m, '1m')

    # 创建线程池，最多维护_njobs个线程
    threadpool = ThreadPoolExecutor(_njobs)
    for _symbol_, _path_list_5m_ in symbol_csv_data_dict_5m.items():
        print(_trade_type, _symbol_)
        # read_csv_resample_2_pkl(_trade_type, _symbol_, _path_list_5m_, symbol_csv_data_dict_1m[_symbol_])
        threadpool.submit(read_csv_resample_2_pkl, _trade_type, _symbol_, _path_list_5m_,
                          symbol_csv_data_dict_1m[_symbol_])
    print("等待线程池读取csv并转换pkl中······")
    threadpool.shutdown(True)  # 等待线程池中的任务执行完毕后，在继续执行
    print("转换pkl结束")


def glob_csv_dict(trade_type, folder, interval):
    path_list = glob(os.path.join(root_path, folder, '*', f'*_{interval}.csv'))
    # ===整理每个币种对应的csv文件路径, 存储到symbol_csv_data_dict中
    symbol_csv_data_dict = dict()
    for x in path_list:
        symbol = os.path.splitext(os.path.basename(x))[0].replace('.csv', '').strip()
        symbol_ = symbol.replace(f'_{interval}', '')

        if trade_type == 'spot':
            # 过滤杠杆代币
            if symbol_ in ['COCOS-USDT', 'BTCST-USDT', 'DREP-USDT', 'SUN-USDT']:
                continue
            if symbol_.endswith(('DOWN-USDT', 'UP-USDT', 'BULL-USDT', 'BEAR-USDT')):
                continue
            if symbol_.endswith('USD'):
                continue
        # if symbol_!='BTC-USDT': continue

        if symbol_ not in symbol_csv_data_dict.keys():
            symbol_csv_data_dict[symbol_] = set()
        symbol_csv_data_dict[symbol_].add(x)
    return symbol_csv_data_dict


def data_center_symbol_process(symbol, trading_type, zip_path_5m, zip_path_1m):
    df_5m = read_symbol_csv(symbol, zip_path_5m)
    agg_dict = {
        'open': 'first',
        'high': 'max',
        'low': 'min',
        'close': 'last',
        'volume': 'sum',
        'quote_volume': 'sum',
        'trade_num': 'sum',
        'taker_buy_base_asset_volume': 'sum',
        'taker_buy_quote_asset_volume': 'sum',
        'avg_price': 'first'
    }
    # =将数据转换为1小时周期
    df = df_5m.resample(rule='1H').agg(agg_dict)

    # =针对1小时数据，补全空缺的数据。保证整张表没有空余数据
    # 对开、高、收、低、价格进行补全处理
    df['close'].fillna(method='ffill', inplace=True)
    df['open'].fillna(value=df['close'], inplace=True)
    df['high'].fillna(value=df['close'], inplace=True)
    df['low'].fillna(value=df['close'], inplace=True)
    # 将停盘时间的某些列，数据填补为0
    fill_0_list = ['volume', 'quote_volume', 'trade_num', 'taker_buy_base_asset_volume',
                   'taker_buy_quote_asset_volume']
    df.loc[:, fill_0_list] = df[fill_0_list].fillna(value=0)

    # 读取1分钟数据
    df_1m = read_symbol_csv(symbol, zip_path_1m)
    df['avg_price_1m'] = df_1m['avg_price']
    df['avg_price_5m'] = df['avg_price']

    # =计算最终的均价
    # 默认使用1分钟均价
    df['avg_price'] = df['avg_price_1m']
    # 没有1分钟均价就使用5分钟均价
    df['avg_price'].fillna(value=df['avg_price_5m'], inplace=True)
    # 没有5分钟均价就使用开盘价
    df['avg_price'].fillna(value=df['open'], inplace=True)
    del df['avg_price_5m'], df['avg_price_1m']
    df.reset_index(inplace=True)
    df['symbol'] = symbol.upper()
    symbol = symbol.upper().replace('USDT', '-USDT')
    if not os.path.exists(os.path.join(pickle_path, f'{trading_type}')):
        os.mkdir(os.path.join(pickle_path, f'{trading_type}'))

    df.to_feather(os.path.join(pickle_path, f'{trading_type}', f'{symbol}.pkl'))
    print('process success', symbol)


def read_symbol_csv(symbol, zip_path):
    zip_list = glob(os.path.join(zip_path, symbol, f'{symbol}*.zip'))
    # 合并monthly daily 数据
    df = pd.concat(
        Parallel(4)(delayed(pd.read_csv)(path_, header=None, encoding="utf-8", compression='zip',
                                         names=['open_time', 'open', 'high', 'low', 'close', 'volume',
                                                'close_time', 'quote_volume', 'trade_num',
                                                'taker_buy_base_asset_volume', 'taker_buy_quote_asset_volume',
                                                'ignore']
                                         ) for path_ in zip_list), ignore_index=True)
    # 过滤表头行
    df = df[df['open_time'] != 'open_time']
    # 规范数据类型，防止计算avg_price报错
    df = df.astype(
        dtype={'open': float, 'high': float, 'low': float, 'close': float, 'volume': float, 'quote_volume': float,
               'trade_num': int, 'taker_buy_base_asset_volume': float, 'taker_buy_quote_asset_volume': float})
    df['avg_price'] = df['quote_volume'] / df['volume']  # 增加 均价
    df['candle_begin_time'] = pd.to_datetime(df['open_time'], unit='ms')
    del df['open_time'], df['close_time'], df['ignore']
    df.sort_values(by='candle_begin_time', inplace=True)  # 排序
    df.drop_duplicates(subset=['candle_begin_time'], inplace=True, keep='last')  # 去除重复值
    df.reset_index(drop=True, inplace=True)  # 重置index
    df.set_index('candle_begin_time', inplace=True)
    return df


def data_center_data_to_pickle_data(trading_type, _njobs):
    zip_path_5m = get_local_path(root_path+sub_path, trading_type, 'klines', 'monthly', None, '5m')
    zip_path_1m = get_local_path(root_path+sub_path, trading_type, 'klines', 'monthly', None, '1m')
    symbols = os.listdir(zip_path_5m)
    # 创建线程池，最多维护_njobs个线程
    threadpool = ThreadPoolExecutor(_njobs)
    for symbol in symbols:
        # data_center_symbol_process(symbol, trading_type, zip_path_5m, zip_path_1m)
        threadpool.submit(data_center_symbol_process, symbol, trading_type, zip_path_5m, zip_path_1m)
    threadpool.shutdown(True)  # 等待线程池中的任务执行完毕后，在继续执行

def run():
    for trade_type in ['spot', 'swap']:
        results_5m = transfer_raw_data_2_pkl_data(trade_type, '5m')
        results_1m = transfer_raw_data_2_pkl_data(trade_type, '1m')
        merge_alldata(results_5m, results_1m, trade_type)
        check_avg(trade_type)


def low_memory_run():
    # njob 为线程数，根据个人配置修改
    njob = cpu_count() - 2
    for trade_type in ['swap']:
        csv_2_pkl(trade_type, '5m', njob)
        check_avg(trade_type)


def data_center_run():
    # njob 为线程数，根据个人配置修改

    njob = cpu_count() - 2
    for trading_type in ['swap']:
        data_center_data_to_pickle_data(trading_type, njob)
        check_avg(trading_type)


if __name__ == '__main__':
    sub_path = '/data/binance-center'
    start_time = datetime.datetime.now()
    # run() # 基于刑大网站数据，原有方法，占用10G以上内存
    # 3选1 run方法
    # low_memory_run() # 基于刑大网站数据, 新增改良方法，占用内存小
    data_center_run() # 基于 0_数据中心.py
    end_time = datetime.datetime.now()
    print("process end cost {} s".format((end_time - start_time).seconds))

