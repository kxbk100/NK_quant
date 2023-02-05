#!/usr/bin/python3
# -*- coding: utf-8 -*-

import pandas as pd
import numpy  as np
import talib
from utils.diff import add_diff, eps


def signal(*args):
    # Dpo
    df = args[0]
    n = args[1]
    diff_num = args[2]
    factor_name = args[3]
    
    '''
    N=20
    DPO=CLOSE-REF(MA(CLOSE,N),N/2+1)
    DPO 是当前价格与延迟的移动平均线的差值，
    通过去除前一段时间的移动平均价格来减少长期的趋势对短期价格波动的影响。
    DPO>0 表示目前处于多头市场;
    DPO<0 表示当前处于空头市场。
    我们通过 DPO 上穿/下穿 0 线来产生买入/卖出信号。
    '''

    df['median'] = df['close'].rolling(
        window=n, min_periods=1).mean()  # 计算中轨
    df[factor_name] = (df['close'] - df['median'].shift(int(n / 2) + 1)) / (df['median'] + eps)

    del df['median']

    if diff_num > 0:
        return add_diff(df, diff_num, factor_name)
    else:
        return df

