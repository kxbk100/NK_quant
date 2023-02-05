#!/usr/bin/python3
# -*- coding: utf-8 -*-

import pandas as pd
import pandas_ta as ta

# https://bbs.quantclass.cn/thread/17756

def signal(*args):
    # SuperTrend
    df = args[0]
    params  = args[1]
    factor_name = args[2]

    length = int(params[0])
    multiplier = float(params[1])

    df.ta.supertrend(high=df['high'], low=df['low'], close=df['close'], length=length, multiplier=multiplier, append=True)

    df[factor_name] = df['SUPERTd_%s_%s' % (length, multiplier)]

    df.drop(['SUPERT_%s_%s' % (length, multiplier), 'SUPERTl_%s_%s' % (length, multiplier), 'SUPERTs_%s_%s' % (length, multiplier)], axis=1, inplace=True)

    return df
