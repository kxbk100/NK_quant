#!/usr/bin/python3
# -*- coding: utf-8 -*-

import pandas as pd
import numpy  as np
from utils.diff import add_diff

def signal(*args):
    # Boll_Count
    df = args[0]
    n = args[1]
    diff_num = args[2]
    factor_name = args[3]

    df['mtm'] = df['close'] / df['close'].shift(n) - 1
    df['mtm'] = df['mtm']*df['quote_volume']/df['quote_volume'].rolling(window=n, min_periods=1).mean()

    #
    df[f'mean'] = df['mtm'].rolling(n).mean()
    df['std'] = df['mtm'].rolling(n).std(ddof=0)
    df['upper'] = df['mean'] + 2 * df['std']
    df['lower'] = df['mean'] - 2 * df['std']
    df['count'] = 0
    df.loc[df['mtm'] > df['upper'], 'count'] = 1
    df.loc[df['mtm'] < df['lower'], 'count'] = -1
    df[factor_name] = df['count'].rolling(n).sum()
    del df['mean']
    del df['std']
    del df['upper']
    del df['lower']
    del df['count']

    if diff_num > 0:
        return add_diff(df, diff_num, factor_name)
    else:
        return df
