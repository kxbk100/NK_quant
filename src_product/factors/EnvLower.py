#!/usr/bin/python3
# -*- coding: utf-8 -*-
import pandas as pd
import numpy  as np
import talib
from utils.diff import add_diff

# =====函数  01归一化
def scale_01(_s, _n):
    _s = (pd.Series(_s) - pd.Series(_s).rolling(_n, min_periods=1).min()) / (
        1e-9 + pd.Series(_s).rolling(_n, min_periods=1).max() - pd.Series(_s).rolling(_n, min_periods=1).min()
    )
    return pd.Series(_s)


def signal(*args):
    # EnvLower
    df = args[0]
    n = args[1]
    diff_num = args[2]
    factor_name = args[3]

    '''
    N=25
    PARAM=0.05 
    MAC=MA(CLOSE,N) 
    UPPER=MAC*(1+PARAM) 
    LOWER=MAC*(1-PARAM)
    ENV(Envolope 包络线)指标是由移动平均线上下平移一定的幅度 (百分比)所得。
    我们知道，价格与移动平均线的交叉可以产生交易信号。
    但是因为市场本身波动性比较强，可能产生很多虚假的交易信号。
    所以我们把移动平均线往上往下平移。
    当价格突破上轨时再产生买入信号或者当价格突破下轨再产生卖出信号。
    这样的方式可以去掉很多假信号
    '''

    lower = (1 - 0.05) * df['close'].rolling(n, min_periods=1).mean()

    signal = lower
    df[factor_name] = scale_01(signal, n)

    if diff_num > 0:
        return add_diff(df, diff_num, factor_name)
    else:
        return df

