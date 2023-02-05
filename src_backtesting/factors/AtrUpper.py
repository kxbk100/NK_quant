#!/usr/bin/python3
# -*- coding: utf-8 -*-
import pandas as pd
import numpy as np
from utils.diff import add_diff, eps


def signal(*args):
    # AtrUpper
    df = args[0]
    n = args[1]
    diff_num = args[2]
    factor_name = args[3]
    
    tr = np.max(np.array([
        (df['high'] - df['low']).abs(),
        (df['high'] - df['close'].shift(1)).abs(),
        (df['low'] - df['close'].shift(1)).abs()
    ]), axis=0)  # 三个数列取其大值
    atr = pd.Series(tr).ewm(alpha=1 / n, adjust=False).mean().shift(1)
    _low = df['low'].rolling(int(n / 2), min_periods=1).min()
    _ma = df['close'].rolling(n, min_periods=1).mean()
    df[factor_name] = (_low + 3 * atr) / (_ma + eps)
    
    if diff_num > 0:
        return add_diff(df, diff_num, factor_name)
    else:
        return df
