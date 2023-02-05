#!/usr/bin/python3
# -*- coding: utf-8 -*-

import pandas as pd
import numpy as np
import talib
from utils.diff import add_diff


def signal(*args):
    # 主买的vwap与当前vwap的比例.
    # https://bbs.quantclass.cn/thread/14374

    df = args[0]
    n  = args[1]
    diff_num = args[2]
    factor_name = args[3]

    df['vwap'] = df['quote_volume'].rolling(n, min_periods=1).sum() / df['volume'].rolling(n, min_periods=1).sum()
    df['buy_vwap'] = df['taker_buy_quote_asset_volume'].rolling(n, min_periods=1).sum() / df['taker_buy_base_asset_volume'].rolling(n, min_periods=1).sum()
    df[factor_name] = df['buy_vwap'] / df['vwap']

    del df['vwap'], df['buy_vwap']

    if diff_num > 0:
        return add_diff(df, diff_num, factor_name)
    else:
        return df