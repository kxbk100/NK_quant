#!/usr/bin/python3
# -*- coding: utf-8 -*-

import numpy  as np
import pandas as pd
import talib as ta
from utils.diff import add_diff, eps


# https://bbs.quantclass.cn/thread/18347


def signal(*args):

    df = args[0]
    n = args[1]
    diff_num = args[2]
    factor_name = args[3]

    # 计算波动率因子
    df['ma'] = df['close'].rolling(window=n, min_periods=1).mean()
    df['trv'] = 100 * ((df['ma'] - df['ma'].shift(n)) / df['ma'].shift(n))
    df[factor_name] = df['trv'].rolling(n, min_periods=1).mean()

    drop_col = [
       'ma', 'trv'
    ]
    df.drop(columns=drop_col, inplace=True)

    if diff_num > 0:
        return add_diff(df, diff_num, factor_name)
    else:
        return df
