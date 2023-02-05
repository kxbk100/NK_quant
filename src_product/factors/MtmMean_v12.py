#!/usr/bin/python3
# -*- coding: utf-8 -*-

import numpy  as np
import pandas as pd
import talib as ta
from utils.diff import add_diff, eps

# https://bbs.quantclass.cn/thread/18957

def signal(*args):
    # MtmMean_v12 指标
    df = args[0]
    n = args[1]
    diff_num = args[2]
    factor_name = args[3]

    df['mtm'] = df['close'] / df['close'].shift(n) - 1
    df['mtm'] = df['mtm']*df['taker_buy_quote_asset_volume']/df['taker_buy_quote_asset_volume'].rolling(window=n, min_periods=1).mean()
    df[factor_name] = df['mtm'].rolling(window=n, min_periods=1).mean()

    if diff_num > 0:
        return add_diff(df, diff_num, factor_name)
    else:
        return df