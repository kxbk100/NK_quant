#!/usr/bin/python3
# -*- coding: utf-8 -*-
import pandas as pd
import numpy  as np
import talib
from utils.diff import add_diff

# https://bbs.quantclass.cn/thread/18152

def signal(*args):

    df = args[0]
    n = args[1]
    diff_num = args[2]
    factor_name = args[3]

    N1 = n
    N2 = 2 * n
    df['ema_1'] = df['close'].ewm(N1, adjust=False).mean()  # EMA(CLOSE,N1)
    df['ema_2'] = df['close'].ewm(N2, adjust=False).mean()  # EMA(CLOSE,N2)
    df['PPO'] = (df['ema_1'] / df['ema_1'].shift(N1) - 1) * abs(df['ema_2'] / df['ema_2'].shift(N2) - 1)

    df[factor_name] = df['PPO'].ewm(N1, adjust=False).mean()  

    del df['ema_1']
    del df['ema_2']
    del df['PPO']

    if diff_num > 0:
        return add_diff(df, diff_num, factor_name)
    else:
        return df










        
