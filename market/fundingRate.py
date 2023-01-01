#!/usr/bin/python3
# -*- coding: utf-8 -*-

import os
import time
import pandas as pd
from datetime import datetime, timedelta
import traceback

from api.market import get_exchange, get_all_symbols, get_funding_rate
from config import root_path


def save(symbol, df):
	k_folder = os.path.join(root_path, 'data', 'funding_rate')
	if not os.path.exists(k_folder):
		os.makedirs(k_folder)
	symbol_ = symbol.replace('USDT', '-USDT')
	if not df.empty:
		df.to_feather(os.path.join(k_folder, f'{symbol_}.pkl'))


if __name__=='__main__':
	host  = '127.0.0.1'
	port  = 7890
	proxy = {'http': 'http://%s:%d' % (host, port), 'https': 'http://%s:%d' % (host, port)}
	# proxy = {}
	exchange   = get_exchange(proxy)

	swap_pickle_path = os.path.join(root_path, 'data', 'pickle_data', 'swap')
	files = [x[2] for x in os.walk(swap_pickle_path)][0]
	symbol_list = [x.split('.')[0].replace('-', '') for x in files]
	symbol_list = [symbol for symbol in symbol_list if symbol.endswith('USDT')]
	symbol_list.sort()

	# symbol_list = get_all_symbols(exchange, 'swap')
	#symbol_list = ['BTCUSDT']

	st = datetime.strptime('2017-09-17 00:00:00', "%Y-%m-%d %H:%M:%S")
	et = datetime.now().replace(minute=0, second=0, microsecond=0)
	# 兼容时区
	utc_offset = int(time.localtime().tm_gmtoff/60/60)
	et = et - timedelta(hours=utc_offset)

	size  = len(symbol_list)
	index = 1
	for symbol in symbol_list:
		print(symbol, '|||| 剩余' + str((size - index)))
		try:
			df = get_funding_rate(exchange, symbol, st, et)
			save(symbol, df)
			time.sleep(1)
			index += 1
		except Exception as e:
			print(traceback.format_exc())
			time.sleep(3)
			index += 1
			continue





