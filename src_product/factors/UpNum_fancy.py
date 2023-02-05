#!/usr/bin/python3
# -*- coding: utf-8 -*-

import pandas as pd
import numpy as np
import talib
from utils.diff import add_diff


def signal(*args):
    # 过去n分钟有多少分钟是上涨的
    # https://bbs.quantclass.cn/thread/14374

    df = args[0]
    n  = args[1]
    diff_num = args[2]
    factor_name = args[3]

    df['_ret_sign'] = df['close'].pct_change() > 0
    df[factor_name] = df['_ret_sign'].rolling(n, min_periods=1).sum()

    del df['_ret_sign']

    if diff_num > 0:
        return add_diff(df, diff_num, factor_name)
    else:
        return df