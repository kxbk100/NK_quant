#!/usr/bin/python3
# -*- coding: utf-8 -*-

import os
import time
import pandas as pd
import numpy  as np
from glob   import glob
from joblib import Parallel, delayed

from config import root_path


def read_one(symbol, path_list, time_interval, _njobs=16):
	symbol_ = symbol.replace(f'_{time_interval}', '')
	df = pd.concat(Parallel(n_jobs=_njobs)(
		delayed(pd.read_csv)(path_, header=1, encoding="GBK", parse_dates=['candle_begin_time'])
			for path_ in path_list), ignore_index=True)
	df.sort_values(by='candle_begin_time', inplace=True) 
	df.drop_duplicates(subset=['candle_begin_time'], inplace=True, keep='last')  # 去除重复值
	df.reset_index(drop=True, inplace=True) 
	return df


def kline_validate(symbol, df, time_interval):
	s_range = pd.date_range(start=df.iat[0, 0], end=df.iat[-1, 0], freq=f'{time_interval[:-1]}T').to_pydatetime()
	k_range = df['candle_begin_time'].dt.to_pydatetime()
	time_df = df[['candle_begin_time']].set_index('candle_begin_time')
	diff_list = sorted(list(set(s_range) - set(k_range)))

	if not diff_list:
		#print(f'{symbol}基准数据完整')
		pass
	else:
		print(f'{symbol}基准数据缺失，先检查补漏{symbol}数据：', diff_list)
		exit()


def run(trade_type, time_interval, white_list=[]):
    trade_type_folder = trade_type if '5m' == time_interval else trade_type + '_1m'
    path_list = glob(os.path.join(root_path, 'data', 'binance', trade_type_folder, '*', f'*_{time_interval}.csv'))

    # ===整理每个币种对应的csv文件路径, 存储到dict中
    symbol_csv_data_dict = dict()
    for x in path_list:
        symbol  = os.path.splitext(os.path.basename(x))[0].replace('.csv', '').strip()
        symbol_ = symbol.replace(f'_{time_interval}', '')
        
        if trade_type == 'spot':
            # 过滤杠杆代币
            if symbol_ in ['COCOS-USDT', 'BTCST-USDT', 'DREP-USDT', 'SUN-USDT']:
                continue
            if symbol_.endswith(('DOWN-USDT', 'UP-USDT', 'BULL-USDT', 'BEAR-USDT')):
                continue
            if symbol_.endswith('USD'):
                continue
        if white_list is not None and len(white_list) > 0:
        	if symbol_ not in white_list: continue

        if symbol not in symbol_csv_data_dict.keys():
            symbol_csv_data_dict[symbol] = set()
        symbol_csv_data_dict[symbol].add(x)

    for symbol, path_list in symbol_csv_data_dict.items():
    	df = read_one(symbol, path_list, time_interval)
    	kline_validate(symbol, df, time_interval)


if __name__=='__main__':
	trade_type = 'swap'
	white_list = ['BTC-USDT']

	for interval in ['5m', '1m']:
		run(trade_type, interval, white_list=[])








