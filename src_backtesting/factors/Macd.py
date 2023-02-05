#!/usr/bin/python3
# -*- coding: utf-8 -*-

import pandas as pd
import numpy  as np
import talib
from utils.diff import add_diff


def signal(*args):
    df = args[0]
    n  = args[1]
    diff_num = args[2]
    factor_name = args[3]
    # 计算macd指标
    '''
    N1=20
    N2=40
    N3=5 
    MACD=EMA(CLOSE,N1)-EMA(CLOSE,N2) 
    MACD_SIGNAL=EMA(MACD,N3) 
    MACD_HISTOGRAM=MACD-MACD_SIGNAL
    MACD 指标衡量快速均线与慢速均线的差值。
    由于慢速均线反映的是之前较长时间的价格的走向，而快速均线反映的是较短时间的价格的走向，
    所以在上涨趋势中快速均线会比慢速均线涨的快，而在下跌趋势中快速均线会比慢速均线跌得快。
    所以 MACD 上穿/下穿 0 可以作为一种构造交易信号的方式。
    另外一种构造交易信号的方式是求 MACD 与其移动平均(信号线)的差值得到 MACD 柱，
    利用 MACD 柱上穿/下穿 0(即 MACD 上穿/下穿其信号线)来构造交易信号。
    这种方式在其他指标的使用中也可以借鉴
    '''
    short_windows = n
    long_windows = 3 * n
    macd_windows = int(1.618 * n)

    df['ema_short'] = df['close'].ewm(span=short_windows, adjust=False).mean()
    df['ema_long']  = df['close'].ewm(span=long_windows, adjust=False).mean()
    df['dif']  = df['ema_short'] - df['ema_long']
    df['dea']  = df['dif'].ewm(span=macd_windows, adjust=False).mean()
    df['macd'] = 2 * (df['dif'] - df['dea'])

    df[factor_name] = df['macd'] / df['macd'].rolling(macd_windows, min_periods=1).mean() - 1

    del df['ema_short']
    del df['ema_long']
    del df['dif']
    del df['dea']
    del df['macd']

    if diff_num > 0:
        return add_diff(df, diff_num, factor_name)
    else:
        return df