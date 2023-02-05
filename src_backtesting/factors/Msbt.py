#!/usr/bin/python3
# -*- coding: utf-8 -*-

import numpy  as np
import pandas as pd
import talib as ta
from utils.diff import add_diff, eps


# https://bbs.quantclass.cn/thread/17753

def signal(*args):

    df = args[0]
    n  = args[1]
    diff_num = args[2]
    factor_name = args[3]

    df['ma'] = df['close'].rolling(window=n).mean()
    df['std'] = df['close'].rolling(n, min_periods=1).std(ddof=0)

    # 收盘价动量
    df['mtm'] = df['close'] / df['close'].shift(n) - 1
    df['mtm'] = df['mtm'].rolling(n, min_periods=1).mean()
  
    # 标准差动量
    df['s_mtm'] = df['std'] / df['std'].shift(n) - 1
    df['s_mtm'] = df['s_mtm'].rolling(n, min_periods=1).mean()

    # bbw波动率
    df['bbw'] = df['std'] / df['ma']
    df['bbw_mean'] = df['bbw'].rolling(window=n).mean()
  
    # taker_buy_quote_asset_volume波动率
    df['volatility'] = df['taker_buy_quote_asset_volume'].rolling(window=n, min_periods=1).sum() / \
                   df['taker_buy_quote_asset_volume'].rolling(window=int(0.5 * n), min_periods=1).sum()

    df[factor_name] = df['mtm'] * df['s_mtm'] * df['bbw_mean'] * df['volatility']

    if diff_num > 0:
        return add_diff(df, diff_num, factor_name)
    else:
        return df
