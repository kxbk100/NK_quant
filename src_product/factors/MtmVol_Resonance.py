#!/usr/bin/python3
# -*- coding: utf-8 -*-

import numpy  as np
import pandas as pd
import talib as ta
from utils.diff import add_diff, eps

# https://bbs.quantclass.cn/thread/18291

def signal(*args):
    # MtmVol_Resonance 指标
    df = args[0]
    n = args[1]
    diff_num = args[2]
    factor_name = args[3]
  
    df['mtm'] = df['close'] / df['close'].shift(n) - 1
    df['mtm_mean'] = df['mtm'].rolling(window=n, min_periods=1).mean()
  
    df['quote_volume_mean'] = df['quote_volume'].rolling(n,min_periods=1).mean()
    df['quote_volume_change'] = (df['quote_volume'] / df['quote_volume_mean'])
    df['quote_volume_change_mean'] = df['quote_volume_change'].rolling(n,min_periods=1).mean()

    df[factor_name] = df['mtm_mean']*df['quote_volume_change_mean']

    drop_col = [
        'mtm', 'mtm_mean','quote_volume_mean', 'quote_volume_change','quote_volume_change_mean'
    ]
    df.drop(columns=drop_col, inplace=True)

    if diff_num > 0:
        return add_diff(df, diff_num, factor_name)
    else:
        return df
