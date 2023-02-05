#!/usr/bin/python3
# -*- coding: utf-8 -*-
import pandas as pd
import numpy as np
from utils.diff import add_diff,eps

# https://bbs.quantclass.cn/thread/18989

def signal(*args):
    # Boll_Count
    df = args[0]
    n = args[1]
    diff_num = args[2]
    factor_name = args[3]
    #
    df['Demax'] = df['high'].diff()  # Demax=HIGH-REF(HIGH,1)；
    df['Demax'] = np.where(df['Demax'] > 0, df['Demax'], 0)  # Demax=IF(Demax>0,Demax,0)
    df['Demin'] = df['low'].shift(1) - df['low']  # Demin=REF(LOW,1)-LOW
    df['Demin'] = np.where(df['Demin'] > 0, df['Demin'], 0)  # Demin=IF(Demin>0,Demin,0)
    df['Ma_Demax'] = df['Demax'].rolling(n, min_periods=1).mean()  # MA(Demax, N)
    df['Ma_Demin'] = df['Demin'].rolling(n, min_periods=1).mean()  # MA(Demin, N)
    df['Demaker'] = df['Ma_Demax'] / (
                df['Ma_Demax'] + df['Ma_Demin'])  # Demaker = MA(Demax, N) / (MA(Demax, N) + MA(Demin, N))
    # df['Demaker_chg'] = df['Demaker']/df

    df['count'] = 0
    df.loc[df['Demaker'] > 0.7, 'count'] = 1
    df.loc[df['Demaker'] < 0.3, 'count'] = -1
    df[factor_name] = df['count'].rolling(n).sum()

    del df['Demax']
    del df['Demin']
    del df['Ma_Demax']
    del df['Ma_Demin']
    del df['Demaker']
    del df['count']

    if diff_num > 0:
        return add_diff(df, diff_num, factor_name)
    else:
        return df