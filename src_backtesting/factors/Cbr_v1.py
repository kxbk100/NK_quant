#!/usr/bin/python3
# -*- coding: utf-8 -*-

import numpy  as np
import pandas as pd
from utils.diff import add_diff

# https://bbs.quantclass.cn/thread/18908

def signal(*args):
    df = args[0]
    n = args[1]
    diff_num = args[2]
    factor_name = args[3]

    # Copp
    df['RC'] = 100 * ((df['close'] - df['close'].shift(n)) / df['close'].shift(n) + (df['close'] - df['close'].shift(2 * n)) / df['close'].shift(2 * n))
    df['RC'] = df['RC'].rolling(n, min_periods=1).mean()

    # bbw
    df['median'] = df['close'].rolling(n, min_periods=1).mean()
    df['std'] = df['close'].rolling(n, min_periods=1).std(ddof=0)
    df['bbw'] = (df['std'] / df['median'])

    # corr
    df['corr'] = ta.CORREL(df['close'], df['volume'], n) + 1
    df['corr'] = df['corr'].rolling(n, min_periods=1).mean()

    df[factor_name] = df['RC'] * df['bbw'] * df['corr']

    del df['RC'], df['median'],  df['std'], df['bbw'], df['corr']

    if diff_num > 0:
        return add_diff(df, diff_num, factor_name)
    else:
        return df
