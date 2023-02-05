#!/usr/bin/python3
# -*- coding: utf-8 -*-

import numpy  as np
import pandas as pd
import talib
from utils.diff import add_diff

# https://bbs.quantclass.cn/thread/18373

def signal(*args):
    df = args[0]
    n  = args[1]
    diff_num = args[2]
    factor_name = args[3]

  
    params = [5, 8, 13, 21, 34, 55, 89]
    df['Fbnq_mean'] = 0
    df['BbwOri'] = 0
    for pn in params:
        # 动量
        df['Fbnq_mean'] += df['close'].ewm(span=pn, adjust=False).mean()
        # 波动率
        df['BbwOri'] += df['close'].rolling(n).std(ddof=0) / df['close'].rolling(n, min_periods=1).mean()
    # 动量
    df['Fbnq_mean'] = df['Fbnq_mean'] / len(params)
    df['Fbnq_mean'] = df['Fbnq_mean'].pct_change(n)

    # 波动率
    df['BbwOri'] = df['BbwOri'] / len(params)

    # 动量 * 波动率
    df[factor_name] = df['Fbnq_mean'] * df['BbwOri']
    del df['Fbnq_mean'], df['BbwOri']

    if diff_num > 0:
        return add_diff(df, diff_num, factor_name)
    else:
        return df