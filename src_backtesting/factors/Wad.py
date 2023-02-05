#!/usr/bin/python3
# -*- coding: utf-8 -*-

import pandas as pd
import numpy  as np
import talib
from utils.diff import add_diff, eps

def signal(*args):
    df = args[0]
    n  = args[1]
    diff_num = args[2]
    factor_name = args[3]

    #  WAD 指标
    """
    TRH=MAX(HIGH,REF(CLOSE,1))
    TRL=MIN(LOW,REF(CLOSE,1))
    AD=IF(CLOSE>REF(CLOSE,1),CLOSE-TRL,CLOSE-TRH) 
    AD=IF(CLOSE==REF(CLOSE,1),0,AD)  
    WAD=CUMSUM(AD)
    N=20
    WADMA=MA(WAD,N)
    参考：https://zhidao.baidu.com/question/19720557.html
    如果今天收盘价大于昨天收盘价，A/D=收盘价减去昨日收盘价和今日最低价较小者；
    如果今日收盘价小于昨日收盘价，A/D=收盘价减去昨日收盘价和今日最高价较大者；
    如果今日收盘价等于昨日收盘价，A/D=0；
    WAD=从第一个交易日起累加A/D；
    """
    df['ref_close'] = df['close'].shift(1)
    df['TRH'] = df[['high', 'ref_close']].max(axis=1)
    df['TRL'] = df[['low', 'ref_close']].min(axis=1)
    df['AD'] = np.where(df['close'] > df['close'].shift(1), df['close'] - df['TRL'], df['close'] - df['TRH'])
    df['AD'] = np.where(df['close'] == df['close'].shift(1), 0, df['AD'])
    df['WAD'] = df['AD'].cumsum()
    df['WADMA'] = df['WAD'].rolling(n, min_periods=1).mean()
    # 去量纲
    df[factor_name] = df['WAD'] / (df['WADMA'] + eps)
    
    del df['ref_close']
    del df['TRH'], df['TRL']
    del df['AD']
    del df['WAD']
    del df['WADMA'] 

    if diff_num > 0:
        return add_diff(df, diff_num, factor_name)
    else:
        return df









