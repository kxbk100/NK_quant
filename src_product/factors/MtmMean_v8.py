#!/usr/bin/python3
# -*- coding: utf-8 -*-

import numpy  as np
import pandas as pd
import talib as ta
from utils.diff import add_diff, eps


# https://bbs.quantclass.cn/thread/18387


def signal(*args):
    # Mtm乘波动率，波动率用最高值与最低值比值表示
    df = args[0]
    n = args[1]
    diff_num = args[2]
    factor_name = args[3]

    df['mtm'] = df['close'] / df['close'].shift(n) - 1
    df['波动'] = df['high'].rolling(n, min_periods=1).max() / df['low'].rolling(
        n, min_periods=1).min() - 1
    df[factor_name] = df['mtm'].rolling(window=n, min_periods=1).mean() * df['波动']

    del df['mtm'], df['波动']

    if diff_num > 0:
        return add_diff(df, diff_num, factor_name)
    else:
        return df