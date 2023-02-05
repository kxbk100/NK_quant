#!/usr/bin/python3
# -*- coding: utf-8 -*-


import pandas as pd
import numpy  as np
import talib
from utils.diff import add_diff

# https://bbs.quantclass.cn/thread/18458

def signal(*args):
    # PMO 指标
    """

    """
    df = args[0]
    n = args[1]
    diff_num = args[2]
    factor_name = args[3]

    df['ma'] = df['close'].rolling(n, min_periods=1).mean()
    df['Bias'] = (df['close'] - df['ma']) / df['ma'] * 100
    df['Bias_DIF'] = df['Bias'] - df['Bias'].shift(3 * n)

    volume = df['quote_volume'].rolling(n, min_periods=1).sum()
    buy_volume = df['taker_buy_quote_asset_volume'].rolling(
        n, min_periods=1).sum()

    df[factor_name] = df['Bias_DIF'].rolling(3 * n + 2, min_periods=1).mean() * (buy_volume / volume)

    del df['ma']
    del df['Bias']
    del df['Bias_DIF']

    if diff_num > 0:
        return add_diff(df, diff_num, factor_name)
    else:
        return df





