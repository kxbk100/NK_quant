#!/usr/bin/python3
# -*- coding: utf-8 -*-
import pandas as pd
import numpy as np
from utils.diff import add_diff, eps


def signal(*args):
    # Hma
    df = args[0]
    n = args[1]
    diff_num = args[2]
    factor_name = args[3]

    '''
    N=20 
    HMA=MA(HIGH,N)
    HMA 指标为简单移动平均线把收盘价替换为最高价。
    当最高价上穿/下穿 HMA 时产生买入/卖出信号
    '''
    hma = df['high'].rolling(n, min_periods=1).mean()
    # 剔除量纲
    df[factor_name] = (df['high'] - hma) / (hma + eps)

    if diff_num > 0:
        return add_diff(df, diff_num, factor_name)
    else:
        return df
