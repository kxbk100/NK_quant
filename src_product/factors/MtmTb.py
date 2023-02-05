#!/usr/bin/python3
# -*- coding: utf-8 -*-

import numpy  as np
import pandas as pd
import talib as ta
from utils.diff import add_diff, eps


# https://bbs.quantclass.cn/thread/18411

def signal(*args):  
    # MtnTb 指标  
    df = args[0]  
    n = args[1]  
    diff_num = args[2]  
    factor_name = args[3]  
  
    # df['tr_trix'] = df['close'].ewm(span=n, adjust=False).mean()  
    # df['tr_pct'] = df['tr_trix'].pct_change()    df['MtmMean'] = (df['close'] / df['close'].shift(n) - 1).ewm(n, adjust=False).mean()  
    # 平均主动买入  
    df['vma'] = df['quote_volume'].rolling(n, min_periods=1).mean()  
    df['taker_buy_ma'] = (df['taker_buy_quote_asset_volume'] / df['vma']) * 100  
    df['taker_buy_mean'] = df['taker_buy_ma'].rolling(window=n).mean()  
  
    df[factor_name] = df['MtmMean'] * df['taker_buy_mean']  
  
    del df['MtmMean'], df['vma'], df['taker_buy_ma'], df['taker_buy_mean']  
  
    if diff_num > 0:  
        return add_diff(df, diff_num, factor_name)  
    else:  
        return df
