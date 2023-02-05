#!/usr/bin/python3
# -*- coding: utf-8 -*-
import pandas as pd
import numpy  as np
from utils.diff import add_diff, eps

# https://bbs.quantclass.cn/thread/17926

def signal(*args):
  
    df = args[0]
    n  = args[1]
    diff_num = args[2]
    factor_name = args[3]
    close_dif = df['close'].diff()
    df['up'] = np.where(close_dif > 0, close_dif, 0)
    df['down'] = np.where(close_dif < 0, abs(close_dif), 0)
    a = df['up'].rolling(n).sum()
    b = df['down'].rolling(n).sum()
    df['rsi'] = a / (a + b + eps)
    df[factor_name] = df['rsi'].rolling(n, min_periods=1).mean()

    # 删除多余列
    del df['up'], df['down'], df['rsi']

    if diff_num > 0:
        return add_diff(df, diff_num, factor_name)
    else:
        return df
