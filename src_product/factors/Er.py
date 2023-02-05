#!/usr/bin/python3
# -*- coding: utf-8 -*-

import numpy  as np
import pandas as pd
import talib as ta
from utils.diff import add_diff, eps


def signal(*args):
    # Er 指标
    df = args[0]
    n  = args[1]
    diff_num = args[2]
    factor_name = args[3]

    '''
    N=20 
    BullPower=HIGH-EMA(CLOSE,N) 
    BearPower=LOW-EMA(CLOSE,N)
    ER 为动量指标, 用来衡量市场的多空力量对比。
    在多头市场，人们会更贪婪地在接近高价的地方买入，BullPower 越高则当前多头力量越强;
    在空头市场，人们可能因为恐惧而在接近低价的地方卖出, BearPower 越低则当前空头力量越强。
    当两者都大于 0 时，反映当前多头力量占据主导地位;
    两者都小于 0 则反映空头力量占据主导地位。 
    如果 BearPower 上穿 0，则产生买入信号; 
    如果 BullPower 下穿 0，则产生卖出信号。
    '''

    a = 2 / (n + 1)
    df['ema'] = df['close'].ewm(alpha=a, adjust=False).mean()
    df['BullPower'] = (df['high'] - df['ema']) / df['ema']
    df['BearPower'] = (df['low'] - df['ema']) / df['ema']
    df[factor_name] = df['BullPower'] + df['BearPower']

    # 删除多余列
    del df['ema'], df['BullPower'], df['BearPower']

    if diff_num > 0:
        return add_diff(df, diff_num, factor_name)
    else:
        return df
