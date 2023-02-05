#!/usr/bin/python3
# -*- coding: utf-8 -*-

import pandas as pd
import numpy  as np
from utils.diff import add_diff

def signal(*args):
    df = args[0]
    n = args[1]
    diff_num = args[2]
    factor_name = args[3]

    # 计算均线
    df['ma_short'] = df['close'].rolling(int(n * 0.618), min_periods=1).mean()
    df['ma_long'] = df['close'].rolling(n, min_periods=1).mean()

    df['gap'] = df['ma_short'] - df['ma_long']
    # 连续k个gap变大才做多， 连续k个gap变小才做空



    df['count'] = 0
    df.loc[df['gap'] > df['gap'].shift(), 'count'] = 1
    df.loc[df['gap'] < df['gap'].shift(), 'count'] = -1
    df[factor_name] = df['count'].rolling(n).sum()

    if diff_num > 0:
        return add_diff(df, diff_num, factor_name)
    else:
        return df
