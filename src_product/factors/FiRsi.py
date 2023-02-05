#!/usr/bin/python3
# -*- coding: utf-8 -*-

import pandas as pd
import numpy  as np
import talib as ta
from utils.diff import add_diff

# https://bbs.quantclass.cn/thread/18739

def signal(*args):
    df = args[0]
    n = args[1]
    diff_num = args[2]
    factor_name = args[3]

    df['_fi'] = df['volume'] * (df['close'] - df['close'].shift(1))

    diff = df['_fi'].diff()
    df['up'] = np.where(diff > 0, diff, 0)
    df['down'] = np.where(diff < 0, abs(diff), 0)
    A = df['up'].rolling(n,min_periods=1).sum()
    B = df['down'].rolling(n,min_periods=1).sum()
    RSI = A / (A + B + eps)

    signal = RSI.ewm(span=n, adjust=False, min_periods=1).mean()
    df[factor_name] = pd.Series(signal)

    if diff_num > 0:
        return add_diff(df, diff_num, factor_name)
    else:
        return df