#!/usr/bin/python3
# -*- coding: utf-8 -*-
import pandas as pd
import numpy as np
import talib as ta
from utils.diff import add_diff, eps

# https://bbs.quantclass.cn/thread/17716

def signal(*args):

    df = args[0]
    n = args[1]
    diff_num = args[2]
    factor_name = args[3]

    df['n日均价'] = df['close'].rolling(n, min_periods=1).mean()
    df['n日标准差'] = df['close'].rolling(n, min_periods=1).std(ddof=0)
    df['n日波动率'] = df['n日标准差'] / df['n日均价']*100
    # 计算上轨、下轨道
    df['RC'] = 100 * ((df['high'] - df['high'].shift(n)) / df['close'].shift(n) + (df['close'] - df['close'].shift(2 * n)) / df['low'].shift(2 * n))
    df['RC_mean'] = df['RC'].rolling(n, min_periods=1).mean()

    # 组合指标
    df[factor_name] = df['RC_mean']*df['n日波动率']

    del  df['n日均价'], df['n日标准差'], df['RC']

    if diff_num > 0:
        return add_diff(df, diff_num, factor_name)
    else:
        return df











