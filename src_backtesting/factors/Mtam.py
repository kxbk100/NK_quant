#!/usr/bin/python3
# -*- coding: utf-8 -*-

import numpy  as np
import pandas as pd
import talib as ta
from utils.diff import add_diff, eps

# https://bbs.quantclass.cn/thread/18410

def signal(*args):
    df = args[0]
    n = args[1]
    diff_num = args[2]
    factor_name = args[3]

    # 计算动量
    df['mtm'] = df['close'] / df['close'].shift(n) - 1

    # 主动成交占比
    volume = df['quote_volume'].rolling(n, min_periods=1).sum()
    buy_volume = df['taker_buy_quote_asset_volume'].rolling(
        n, min_periods=1).sum()
    df['taker_by_ratio'] = buy_volume / volume

    # 波动率因子
    df['c1'] = df['high'] - df['low']
    df['c2'] = abs(df['high'] - df['close'].shift(1))
    df['c3'] = abs(df['low'] - df['close'].shift(1))
    df['tr'] = df[['c1', 'c2', 'c3']].max(axis=1)
    df['atr'] = df['tr'].rolling(window=n, min_periods=1).mean()
    df['avg_price_'] = df['close'].rolling(window=n, min_periods=1).mean()
    df['wd_atr'] = df['atr'] / df['avg_price_']

    # 动量 * 主动成交占比 * 波动率
    df['mtm'] = df['mtm'] * df['taker_by_ratio'] * df['wd_atr']
    df[factor_name] = df['mtm'].rolling(window=n, min_periods=1).mean()

    drop_col = [
        'mtm', 'taker_by_ratio', 'c1', 'c2', 'c3', 'tr', 'atr', 'wd_atr', 'avg_price_'
    ]
    df.drop(columns=drop_col, inplace=True)

    if diff_num > 0:
        return add_diff(df, diff_num, factor_name)
    else:
        return df
