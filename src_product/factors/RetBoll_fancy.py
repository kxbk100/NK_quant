#!/usr/bin/python3
# -*- coding: utf-8 -*-

import pandas as pd
import numpy as np
import talib
from utils.diff import add_diff


def signal(*args):
    # 布林线应用到收益率上
    # https://bbs.quantclass.cn/thread/14374

    df = args[0]
    n  = args[1]
    diff_num = args[2]
    factor_name = args[3]

    df['_ret'] = df['close'].pct_change()
    df[factor_name] =(df['_ret'] - df['_ret'].rolling(n, min_periods=1).mean()) / df['_ret'].rolling(n, min_periods=1).std()

    del df['_ret']

    if diff_num > 0:
        return add_diff(df, diff_num, factor_name)
    else:
        return df