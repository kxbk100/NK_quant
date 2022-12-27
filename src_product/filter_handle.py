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

    df1 = pd.merge(df1,
        fundingrate_data[['candle_begin_time', 'symbol', 'fundingRate']],
        on=['candle_begin_time', 'symbol'], how="left")


    df1['费率max_fl_24'] = df1['fundingRate'].rolling(24).max()

    df2['费率min_fl_24'] = df2['fundingRate'].rolling(24).min()

    rate = ['fundingRate'][0]
    feature = ['费率min_fl_24'][0]

    df2[feature + '升序'] = df2.groupby('candle_begin_time')[feature].apply(
                    lambda x: x.rank(pct=False, ascending=True, method='first'))
    df2 = df2[(df2[feature + '升序'] >= 10) | (df2[rate] >= 0)]

    feature = ['费率max_fl_24'][0]
    df1[feature + '降序'] = df1.groupby('candle_begin_time')[feature].apply(
                    lambda x: x.rank(pct=False, ascending=False, method='first'))
    df1 = df1[(df1[feature + '降序'] >= 10) | (df1[rate] <= 0)]


    return df1, df2

