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

    #
    df[f'mean'] = df['close'].rolling(n).mean()
    df['std'] = df['close'].rolling(n).std(ddof=0)
    df['upper'] = df['mean'] + 2 * df['std']
    df['lower'] = df['mean'] - 2 * df['std']
    df['count'] = 0
    df.loc[df['close'] > df['upper'], 'count'] = 1
    df.loc[df['close'] < df['lower'], 'count'] = -1
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
