#!/usr/bin/python3
# -*- coding: utf-8 -*-

import pandas as pd
import numpy as np
import talib
from utils.diff import add_diff


def signal(*args):
    # 当前价格与过去N分钟的最高价最低价之比，看上涨还是下跌的动力更强
    # https://bbs.quantclass.cn/thread/14374

    df = args[0]
    n  = args[1]
    diff_num = args[2]
    factor_name = args[3]

    df[factor_name] = -1 * df['low'].rolling(n, min_periods=1).min() / df['close'] - df['high'].rolling(n, min_periods=1).max() / df['close']

    if diff_num > 0:
        return add_diff(df, diff_num, factor_name)
    else:
        return df