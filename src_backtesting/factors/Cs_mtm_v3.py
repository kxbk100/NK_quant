#!/usr/bin/python3
# -*- coding: utf-8 -*-

import numpy  as np
import pandas as pd
import talib as ta
from utils.diff import add_diff, eps


# https://bbs.quantclass.cn/thread/17641


def signal(*args):
    # Cs_mtm_v3 指标
    df = args[0]
    n  = args[1]
    diff_num = args[2]
    factor_name = args[3]
    # 收盘价动量
    df['c_mtm'] = df['close'] / df['close'].shift(n) - 1
    df['c_mtm'] = df['c_mtm'].rolling(n, min_periods=1).mean()
    df['std'] = df['close'].rolling(n, min_periods=1).std(ddof=0)
    # 标准差动量
    df['s_mtm'] = df['std'] / df['std'].shift(n) - 1
    df['s_mtm'] = df['s_mtm'].rolling(n, min_periods=1).mean()
    df[factor_name] = df['c_mtm'] * df['s_mtm']

    del df['c_mtm'], df['std'], df['s_mtm']

    if diff_num > 0:
        return add_diff(df, diff_num, factor_name)
    else:
        return df
