#!/usr/bin/python3
# -*- coding: utf-8 -*-

import numpy  as np
import pandas as pd
import talib as ta
from utils.diff import add_diff


def signal(*args):
    # Vidya_v2
    df = args[0]
    n  = args[1]
    diff_num = args[2]
    factor_name = args[3]

    '''
    N=10 
    VI=ABS(CLOSE-REF(CLOSE,N))/SUM(ABS(CLOSE-REF(CLOSE ,1)),N)
    VIDYA=VI*CLOSE+(1-VI)*REF(CLOSE,1)
    VIDYA 也属于均线的一种，不同的是，VIDYA 的权值加入了 ER (EfficiencyRatio)指标。
    在当前趋势较强时，ER 值较大，VIDYA 会赋予当前价格更大的权重，
    使得 VIDYA 紧随价格变动，减小其滞后性;
    在当前趋势较弱(比如振荡市中)，ER 值较小，VIDYA 会赋予当前价格较小的权重，
    增大 VIDYA 的滞后性，使其更加平滑，避免产生过多的交易信号。
    当收盘价上穿/下穿 VIDYA 时产生买入/卖出信号。
    '''

    _ts = (df['open'] + df['close']) / 2.

    _vi = (_ts - _ts.shift(n)).abs() / (
        _ts - _ts.shift(1)).abs().rolling(n, min_periods=1).sum()
    _vidya = _vi * _ts + (1 - _vi) * _ts.shift(1)

    df[factor_name] = pd.Series(_vidya)

    if diff_num > 0:
        return add_diff(df, diff_num, factor_name)
    else:
        return df
