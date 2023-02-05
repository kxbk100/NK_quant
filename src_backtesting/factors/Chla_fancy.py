#!/usr/bin/python3
# -*- coding: utf-8 -*-

import pandas as pd
import numpy as np
import talib
from utils.diff import add_diff


def signal(*args):
    # https://bbs.quantclass.cn/thread/14374

    df = args[0]
    n  = args[1]
    diff_num = args[2]
    factor_name = args[3]

    df['divnum'] = df['high'] - df['low']
    df['divnum'] = df['divnum'].replace(0, np.nan)
    df['temp'] = (2 * df['close'] - df['high'] - df['low']) / df['divnum'] * df['quote_volume']
    df[factor_name] = df['temp'].rolling(n, min_periods=1).sum()

    del df['divnum'], df['temp']

    if diff_num > 0:
        return add_diff(df, diff_num, factor_name)
    else:
        return df