#!/usr/bin/python3
# -*- coding: utf-8 -*-

import numpy  as np
import pandas as pd
import talib as ta
from utils.diff import add_diff

# https://bbs.quantclass.cn/thread/18266

def signal(args):
 
     df, n, diff_num, factor_name = args
 
     n1 = int(n)
 
     # ==============================================================
     ts = df[['high', 'low']].sum(axis=1) / 2
 
     close_ma = ts.rolling(n, min_periods=1).mean()
     tma = close_ma.rolling(n, min_periods=1).mean()
     df['mtm'] = df['close'] / (tma+eps) - 1
 
     df['mtm_mean'] = df['mtm'].rolling(window=n1, min_periods=1).mean()
 
     # 基于价格atr，计算波动率因子wd_atr
     df['c1'] = df['high'] - df['low']
     df['c2'] = abs(df['high'] - df['close'].shift(1))
     df['c3'] = abs(df['low'] - df['close'].shift(1))
     df['tr'] = df[['c1', 'c2', 'c3']].max(axis=1)
     df['atr'] = df['tr'].rolling(window=n1, min_periods=1).mean()
     df['avg_price'] = df['close'].rolling(window=n1, min_periods=1).mean()
     df['wd_atr'] = df['atr'] / df['avg_price']
 
     # 平均主动买入
     df['vma'] = df['quote_volume'].rolling(n, min_periods=1).mean()
     df['taker_buy_ma'] = (df['taker_buy_quote_asset_volume'] / df['vma'])  100
     df['taker_buy_mean'] = df['taker_buy_ma'].rolling(window=n).mean()
 
     indicator = 'mtm_mean'
 
     # mtm_mean指标分别乘以三个波动率因子
     df[indicator] = df[indicator]  df['wd_atr']  df['taker_buy_mean']
     df[factor_name] = df[indicator] * 100000000
 
     drop_col = [
         'mtm', 'mtm_mean', 'c1', 'c2', 'c3', 'tr', 'atr', 'wd_atr','avg_price_',
         'vma' ,'taker_buy_ma','taker_buy_mean'
     ]
     df.drop(columns=drop_col, inplace=True)
 
     if diff_num > 0:
         return add_diff(df, diff_num, factor_name)
     else:
         return df
 