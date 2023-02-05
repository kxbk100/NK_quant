#!/usr/bin/python3
# -*- coding: utf-8 -*-


import pandas as pd
import numpy  as np
import talib
from utils.diff import add_diff

# https://bbs.quantclass.cn/thread/17821

def signal(*args):

    df = args[0]
    n = args[1]
    diff_num = args[2]
    factor_name = args[3]

    # COPP
    # RC=100*((CLOSE-REF(CLOSE,N1))/REF(CLOSE,N1)+(CLOSE-REF(CLOSE,N2))/REF(CLOSE,N2))
    df['RC'] = 100 * ((df['close'] - df['close'].shift(n)) / df['close'].shift(n) + (df['close'] - df['close'].shift(2 * n)) / df['close'].shift(2 * n))
    df['RC_mean'] = df['RC'].rolling(n, min_periods=1).mean()

    # ATR
    df['median'] = df['close'].rolling(window=n).mean()
    df['c1'] = df['high'] - df['low']  # HIGH-LOW
    df['c2'] = abs(df['high'] - df['close'].shift(1))  # ABS(HIGH-REF(CLOSE,1)
    df['c3'] = abs(df['low'] - df['close'].shift(1))  # ABS(LOW-REF(CLOSE,1))
    df['TR'] = df[['c1', 'c2', 'c3']].max(axis=1)  # TR=MAX(HIGH-LOW,ABS(HIGH-REF(CLOSE,1)),ABS(LOW-REF(CLOSE,1)))
    df['_ATR'] = df['TR'].rolling(n, min_periods=1).mean()  # ATR=MA(TR,N)
    # ATR指标去量纲
    df['ATR'] = df['_ATR'] / df['median']

    # 平均主动买入
    df['vma'] = df['quote_volume'].rolling(n, min_periods=1).mean()
    df['taker_buy_ma'] = (df['taker_buy_quote_asset_volume'] / df['vma']) * 100
    df['taker_buy_mean'] = df['taker_buy_ma'].rolling(window=n).mean()

    # 组合指标
    df[factor_name] = df['RC_mean'] * df['ATR'] * df['taker_buy_mean']
    # 删除多余列
    del df['RC'], df['RC_mean']
    del df['median'], df['c1'], df['c2'], df['c3'], df['TR'], df['_ATR'], df['ATR']
    del df['vma'], df['taker_buy_ma'], df['taker_buy_mean']

    if diff_num > 0:
        return add_diff(df, diff_num, factor_name)
    else:
        return df
