#!/usr/bin/python3
# -*- coding: utf-8 -*-


import pandas as pd
import numpy  as np
import talib
from utils.diff import add_diff


def signal(*args):
    # Bias
    df = args[0]
    n = args[1]
    diff_num = args[2]
    factor_name = args[3]


    df['route_1'] = (df['high'] - df['open']) + ( df['high'] - df['low']) + ( df['close'] - df['low'] )
    df['route_2'] = (df['open'] - df['low']) + ( df['high'] - df['low']) + (df['high'] - df['close'])
    df['min_route']  = df[['route_1','route_2']].min(axis=1)/df['open'] #  最短路径归一化

    df['RC'] = 100 * (df['close'] / df['close'].shift(n) - 1 + df['close'] / df['close'].shift(2 * n) - 1)
    df['RC'] = df['RC'].ewm(n, adjust=False).mean()
    df['min_route'] = df['min_route'].ewm(n, adjust=False).mean()
    df[factor_name] = df['RC'] / (df['min_route']+eps)

    if diff_num > 0:
        return add_diff(df, diff_num, factor_name)
    else:
        return df