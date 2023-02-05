#!/usr/bin/python3
# -*- coding: utf-8 -*-

import numpy  as np
import pandas as pd
import talib as ta
from utils.diff import add_diff, eps


# https://bbs.quantclass.cn/thread/17771

def signal(*args):
    # MtmTBull
    df = args[0]
    n = args[1]
    diff_num = args[2]
    factor_name = args[3]

    # 动量
    df['ma'] = df['close'].rolling(window=n).mean()
    df['mtm'] = (df['close'] / df['ma'].shift(n) - 1) * 100
    df['mtm_mean'] = df['mtm'].rolling(window=n).mean()

    # 平均波幅
    df['tr1'] = df['high'] - df['low']
    df['tr2'] = abs(df['high'] - df['close'].shift(1))
    df['tr3'] = abs(df['low'] - df['close'].shift(1))
    df['tr'] = df[['tr1', 'tr2', 'tr3']].max(axis=1)
    df['ATR_abs'] = df['tr'].rolling(window=n, min_periods=1).mean()
    df['ATR'] = df['ATR_abs'] / df['ma'] * 100

    # 平均主动买入
    df['vma'] = df['quote_volume'].rolling(n, min_periods=1).mean()
    df['taker_buy_ma'] = (df['taker_buy_quote_asset_volume'] / df['vma']) * 100
    df['taker_buy_mean'] = df['taker_buy_ma'].rolling(window=n).mean()

    # 组合指标
    df[factor_name] = df['mtm_mean'] * df['ATR'] * df['taker_buy_mean']

    drop_col = [
        'ma', 'mtm', 'mtm_mean', 'tr1', 'tr2', 'tr3', 'tr', 'ATR_abs', 'ATR',
        'vma', 'taker_buy_ma', 'taker_buy_mean',
    ]
    df.drop(columns=drop_col, inplace=True)

    if diff_num > 0:
        return add_diff(df, diff_num, factor_name)
    else:
        return df
