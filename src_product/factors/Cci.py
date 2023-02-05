#!/usr/bin/python3
# -*- coding: utf-8 -*-

import numpy  as np
import pandas as pd
from utils.diff import add_diff, eps


def signal(*args):
    # CCI 最常用的T指标
    df = args[0]
    n  = args[1]
    diff_num = args[2]
    factor_name = args[3]
    
    '''
    N=14 
    TP=(HIGH+LOW+CLOSE)/3 
    MA=MA(TP,N) 
    MD=MA(ABS(TP-MA),N) 
    CCI=(TP-MA)/(0.015MD)
    CCI 指标用来衡量典型价格(最高价、最低价和收盘价的均值)与其一段时间的移动平均的偏离程度。
    CCI 可以用来反映市场的超买超卖状态。
    一般认为，CCI 超过 100 则市场处于超买状态；CCI 低于 -100 则市场处于超卖状态。
    当 CCI 下穿 100/上穿-100 时，说明股价可能要开始发生反转，可以考虑卖出/买入。
    '''

    df['tp'] = (df['high'] + df['low'] + df['close']) / 3
    df['ma'] = df['tp'].rolling(window=n, min_periods=1).mean()
    df['md'] = abs(df['close'] - df['ma']).rolling(window=n, min_periods=1).mean()
    df[factor_name] = (df['tp'] - df['ma']) / (df['md'] * 0.015 + eps)

    del df['tp']
    del df['ma']
    del df['md']

    if diff_num > 0:
        return add_diff(df, diff_num, factor_name)
    else:
        return df
