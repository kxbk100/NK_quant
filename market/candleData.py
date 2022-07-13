#!/usr/bin/python3
# -*- coding: utf-8 -*-

import os
import time
import ccxt
import pandas as pd
import numpy  as np
from datetime import datetime, timedelta

from api.market    import get_exchange, get_all_symbols, get_klines
from utils.commons import robust
from config import root_path, header_columns


def save_csv(df, symbol, trade_type, time_interval):
	k_folder = os.path.join(root_path, 'data', 'binance')
	if time_interval=='1m':
		k_folder = os.path.join(k_folder, f'{trade_type}_1m')
	else:
		k_folder = os.path.join(k_folder, f'{trade_type}')

	df.reset_index(drop=True, inplace=True)
	df.set_index('candle_begin_time', inplace=True)
	for index, df_ in df.groupby(pd.Grouper(freq='D')):
		symbol_ = symbol.replace('USDT', '-USDT')

		year  = str(index.year)
		month = str(('0%d' % index.month) if index.month < 10 else index.month)
		day   = str(('0%d' % index.day) if index.day < 10 else index.day)
		save_path = os.path.join(k_folder, f'{year}-{month}-{day}')
		if not os.path.exists(save_path):
			os.makedirs(save_path)

		filepath = os.path.join(save_path, f'{symbol_}_{time_interval}.csv')
		# 如果存在, 先删除之前的
		if os.path.exists(filepath): os.remove(filepath)
		with open(filepath, "w") as f:
			f.write('xbx\n')

		df_ = df_.copy()
		df_['candle_begin_time'] = df_.index
		df_ = df_[header_columns]
		df_.reset_index(drop=True, inplace=True)
		df_.to_csv(filepath, index=False, mode="a")


if __name__=='__main__':
	trade_type  = 'swap'
	period_list = ['1m', '5m', '1h']

	host  = '127.0.0.1'
	port  = 9910
	proxy = {'http': 'http://%s:%d' % (host, port), 'https': 'http://%s:%d' % (host, port)}
	exchange    = get_exchange(proxy)
	symbol_list = get_all_symbols(exchange, trade_type)
	#symbol_list = ['BTCUSDT', ]

	start_dt   = datetime.strptime('2017-09-17 00:00:00', "%Y-%m-%d %H:%M:%S")
	end_dt     = datetime.now().replace(minute=0, second=0, microsecond=0) - timedelta(hours=1)
	#start_dt   = datetime.strptime('2021-12-13 00:00:00', "%Y-%m-%d %H:%M:%S")
	#end_dt     = datetime.strptime('2021-12-14 00:00:00', "%Y-%m-%d %H:%M:%S")

	print(f'******** {trade_type} ********')
	print('period_list:', period_list)
	print()
	size  = len(symbol_list)
	index = 1
	for symbol in symbol_list:
		print(symbol, '|||| 剩余' + str((size - index)))
		for period_ in period_list:
			print('        ---', period_)
			df = get_klines(exchange, symbol, start_dt, end_dt, trade_type, interval=period_)
			save_csv(df, symbol, trade_type, period_)
			time.sleep(1)
		index += 1
