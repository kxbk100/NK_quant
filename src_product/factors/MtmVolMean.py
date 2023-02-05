#!/usr/bin/python3
# -*- coding: utf-8 -*-

import numpy  as np
import pandas as pd
import talib as ta
from utils.diff import add_diff, eps


# https://bbs.quantclass.cn/thread/18140

def signal(*args):
    # MtmVolMean 指标
    df = args[0]
    n = args[1]
    diff_num = args[2]
    factor_name = args[3]

    df['close_change'] = (df['close'] / df['close'].shift(n) - 1).ewm(n, adjust=False).mean() * 100
    df['vol_change'] = (df['quote_volume'] / df['quote_volume'].shift(n) - 1).ewm(n, adjust=False).mean() * 100

    df[factor_name] = df['close_change'] * df['vol_change']

    if diff_num > 0:
        return add_diff(df, diff_num, factor_name)
    else:
        return df