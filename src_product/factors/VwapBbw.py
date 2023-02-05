#!/usr/bin/python3
# -*- coding: utf-8 -*-

import pandas as pd
import numpy  as np
import talib
from utils.diff import add_diff


# https://bbs.quantclass.cn/thread/18129

def signal(*args):
    # VwapBbw
    df = args[0]
    n = args[1]
    diff_num = args[2]
    factor_name = args[3]
  
    vwap = df['quote_volume'] / df['volume']
    vwap_chg = vwap.pct_change(n)
    # 计算宽度的变化率
    width = df['close'].rolling(n, min_periods=1).std(ddof=0) * 2
    avg = df['close'].rolling(n, min_periods=1).mean()
    top = avg + width
    bot = avg - width
    bbw = top / bot
    bbw_chg = bbw.pct_change(n)

    df['成交额归一'] = df['quote_volume'] / df['quote_volume'].rolling(n, min_periods=1).mean()

    feature = (vwap_chg * bbw_chg) / df['成交额归一']
    df[factor_name] = feature.rolling(n, min_periods=1).sum()

    del df['成交额归一']

    if diff_num > 0:
        return add_diff(df, diff_num, factor_name)
    else:
        return df
