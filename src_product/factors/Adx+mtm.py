#!/usr/bin/python3
# -*- coding: utf-8 -*-

import numpy  as np
import pandas as pd
import talib as ta
from utils.diff import add_diff

# https://bbs.quantclass.cn/thread/18446

def signal(*args):

    df = args[0]
    n  = args[1]
    diff_num = args[2]
    factor_name = args[3]

    df['max_high'] = np.where(df['high'] > df['high'].shift(1), df['high'] - df['high'].shift(1), 0)

    df['max_low'] = np.where(df['low'].shift(1) > df['low'], df['low'].shift(1) - df['low'], 0)
    df['XPDM'] = np.where(df['max_high'] > df['max_low'], df['high'] - df['high'].shift(1), 0)
    df['PDM'] = df['XPDM'].rolling(n).sum()

    df['c1'] = abs(df['high'] - df['low'])
    df['c2'] = abs(df['high'] - df['close'])
    df['c3'] = abs(df['low'] - df['close'])
    df['TR'] = df[['c1', 'c2', 'c3']].max(axis=1)

    df['TR_sum'] = df['TR'].rolling(n).sum()
    df['DI+'] = df['PDM'] / df['TR_sum']

    df['mtm'] = (df['close'] / df['close'].shift(n) - 1).rolling(
        window=n, min_periods=1).mean()

    df[factor_name] = df['DI+'] * df['mtm']

    del df['max_high']
    del df['max_low']
    del df['XPDM']
    del df['PDM']
    del df['mtm']
    del df['c1']
    del df['c2']
    del df['c3']
    del df['TR']
    del df['TR_sum']
    del df['DI+']

    if diff_num > 0:
        return add_diff(df, diff_num, factor_name)
    else:
        return df