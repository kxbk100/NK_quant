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

    df1, df2 = filter_fundingrate(df1, df2)

    return df1, df2


def filter_fundingrate(df1, df2):
    df2 = df2[df2['fundingRate']>-0.025]

    feature = ['费率min_fl_24'][0]

    df2[feature + '升序'] = df2.groupby('candle_begin_time')[feature].apply(
                    lambda x: x.rank(pct=False, ascending=True, method='first'))
    df2 = df2[(df2[feature + '升序'] >= 2) | (df2[feature] >= -0.01)]

    feature = ['费率max_fl_24'][0]
    df1[feature + '降序'] = df1.groupby('candle_begin_time')[feature].apply(
                    lambda x: x.rank(pct=False, ascending=False, method='first'))
    df1 = df1[(df1[feature + '降序'] >= 2) | (df1[feature] <= 0.01)]


    return df1, df2


