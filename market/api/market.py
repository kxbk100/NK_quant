#!/usr/bin/python3
# -*- coding: utf-8 -*-

import os
import time
import ccxt
import pandas as pd
import numpy  as np
from datetime import datetime, timedelta
import traceback

from utils.commons import robust
from config import header_columns


def get_exchange(proxy={}):
	exchange = ccxt.binance()
	exchange.proxies = proxy
	return exchange


def get_all_symbols(exchange, trade_type):
	if trade_type== 'swap':
		exchange_info = robust(exchange.fapiPublic_get_exchangeinfo,)  # 获取账户净值
	elif trade_type== 'spot':
		exchange_info = robust(exchange.public_get_exchangeinfo,)
	else:
		raise ValuseError(f'trade_type {trade_type} error!!!')

	_symbol_list  = [x['symbol'] for x in exchange_info['symbols']]
	symbol_list   = [symbol for symbol in _symbol_list if symbol.endswith('USDT')]	
	symbol_list.sort()
	return symbol_list


def get_funding_rate(exchange, symbol, start_time, end_time, retry_times=5):
	st = int(time.mktime(start_time.timetuple())) * 1000
	et = int(time.mktime(end_time.timetuple())) * 1000
	params = {
		'symbol':    symbol,
		'startTime': st,
		'end_time':  et,
		'limit':     1000,
	}
	alldata = pd.DataFrame()
	while True:
		try:
			for _ in range(retry_times):
				try:
					data = exchange.fapiPublicGetFundingRate(params)
				except Exception as e:
					if _ >= (retry_times - 1):
						raise e

			df = pd.DataFrame(data, columns=['symbol', 'fundingRate', 'fundingTime'], dtype='float')
			df['fundingTime'] = (df['fundingTime']//1000) * 1000
			df['fundingTime'] = pd.to_datetime(df['fundingTime'], unit='ms')
			df.rename(columns={'fundingTime':'candle_begin_time'}, inplace=True)
			df = df[['candle_begin_time', 'symbol', 'fundingRate']]
			df.sort_values(by='candle_begin_time', inplace=True)
			df.reset_index(drop=True, inplace=True)

			alldata = alldata.append(df, ignore_index=False, sort=False)

			# 更新请求参数
			curr_st = int(df.iat[-1, 0].timestamp()) * 1000
			params['startTime'] = curr_st
			shift_ = (et - params['startTime'])//1000/3600 
			
			if  (shift_ > 0 and shift_ < 8) or df.shape[0] <= 1:
				break
			else:
				time.sleep(1)
		except Exception as e:
			time.sleep(3)
			print(traceback.format_exc())
			params['startTime'] = st
			alldata = pd.DataFrame()
			continue

	alldata['candle_begin_time'] = alldata['candle_begin_time'] - pd.Timedelta(hours=8) 
	alldata.sort_values(by='candle_begin_time', inplace=True)
	alldata.drop_duplicates(subset=['candle_begin_time'], inplace=True, keep='last') 

	st = alldata.iloc[0]['candle_begin_time'].strftime("%Y-%m-%d %H:%M:%S")
	et = end_time.strftime("%Y-%m-%d %H:%M:%S")
	mux = pd.MultiIndex.from_product([pd.date_range(start=st, end=et, freq='1H'), df['symbol'].unique()], names=['candle_begin_time', 'symbol'])
	alldata = alldata.set_index(['candle_begin_time', 'symbol']).reindex(mux)
	alldata['fundingRate'].fillna(method='ffill', inplace=True)
	alldata.reset_index(inplace=True)
	return alldata


def get_klines(exchange, symbol, start_time_dt, end_time_dt, trade_type, interval='1h', limit=1500, retry_times=5):
	datetime_format = "%Y-%m-%d %H:%M:%S"
	header_columns = [
		'candle_begin_time', 
		'symbol', 
		'open', 
		'high', 
		'low', 
		'close', 
		'volume', 
		'quote_volume',
	    'trade_num',
	    'taker_buy_base_asset_volume', 
	    'taker_buy_quote_asset_volume',
	]
	columns = [
		'candle_begin_time', 
		'open', 
		'high', 
		'low', 
		'close', 
		'volume', 
		'close_time', 
		'quote_volume', 
		'trade_num',
		'taker_buy_base_asset_volume', 
		'taker_buy_quote_asset_volume', 
		'ignore'
	]
	# 处理时区兼容
	utc_offset  = int(time.localtime().tm_gmtoff/60/60)
	# 13位
	start_time_t = int(time.mktime(start_time_dt.timetuple())) * 1000
	end_time_t   = int(time.mktime(end_time_dt.timetuple())) * 1000
	params = {
		'symbol':    symbol, 
		'interval':  interval, 
		'limit':     limit,
		'startTime': start_time_t
	}
	alldata = pd.DataFrame()
	while True:
		try:
			for _ in range(retry_times):
				try:
					if trade_type=='swap':
						kline = exchange.fapiPublic_get_klines(params)
					elif trade_type=='spot':
						kline = exchange.public_get_klines(params)
					else:
						print(f'trade_type {trade_type} error!!!')
						exit()
					break
				except Exception as e:
					if _ >= (retry_times - 1):
						raise e

			# 整理数据
			df = pd.DataFrame(kline, columns=columns, dtype='float')
			df['candle_begin_time'] = pd.to_datetime(df['candle_begin_time'], unit='ms')

			alldata = alldata.append(df, ignore_index=False, sort=False)
			# 更新请求参数
			params['startTime'] = int(df.iat[-1, 0].timestamp()) * 1000
			if params['startTime'] >= end_time_t or df.shape[0] <= 1:
				break
			else:
				time.sleep(1)
		except Exception as e:
			time.sleep(5)
			print(e)
			params['startTime'] = start_time_t
			alldata = pd.DataFrame()
			continue

	# 只保留当日数据
	alldata = alldata[alldata['candle_begin_time'] <= end_time_dt]
	alldata.sort_values('candle_begin_time', inplace=True)
	alldata.drop_duplicates(subset=['candle_begin_time'], keep='last', inplace=True)
	alldata.reset_index(drop=True, inplace=True)
	
	return alldata



