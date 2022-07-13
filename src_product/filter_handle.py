#!/usr/bin/python3
# -*- coding: utf-8 -*-

import pandas as pd


def default_handler(df1, df2):
	return df1, df2


def filter_before(df1, df2):
	#"""
	filter_factor = 'AdaptBolling_fl_100'
	# 破下轨不做多
	df1 = df1[~(df1[filter_factor] == -1)]
	# 破上轨不做空
	df2 = df2[~(df2[filter_factor] ==  1)]
	#"""

	return df1, df2


def filter_fundingrate(df1, df2):
	from functions import get_fundingrate
	# 整合资金费率数据
	fundingrate_data = get_fundingrate()
	df2 = pd.merge(df2, 
		fundingrate_data[['candle_begin_time', 'symbol', 'fundingRate']], 
		on=['candle_begin_time', 'symbol'], how="left")

	# 资金费率为负不做空
	df2 = df2[df2['fundingRate']>=0]

	return df1, df2