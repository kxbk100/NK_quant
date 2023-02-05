#!/usr/bin/python3
# -*- coding: utf-8 -*-

import numpy  as np
import pandas as pd
import talib as ta
from utils.diff import add_diff, eps


# https://bbs.quantclass.cn/thread/17962

def signal(*args):
    '''
    以hint构建作为选B因子
    '''
    df = args[0]
    n = args[1]
    diff_num = args[2]
    factor_name = args[3]
    # ==============================================================



    df['mtm'] = df['high'] / df['high'].shift(n) - 1
    df['mtm_mean'] = df['mtm'].rolling(window=n, min_periods=1).mean()
    df['ma'] = df['close'].rolling(n, min_periods=1).mean()
    df['cm'] = df['close'] / df['ma']
    df[factor_name] = (df['mtm_mean'] - df['cm']) / df['cm']
  
    if diff_num > 0:
        return add_diff(df, diff_num, factor_name)
    else:
        return df
