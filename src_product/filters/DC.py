#!/usr/bin/python3
# -*- coding: utf-8 -*-

import pandas as pd
import numpy  as np


def signal(*args):
    # DC
    df = args[0]
    n  = args[1]
    factor_name = args[2]

    df['up'] = df['high'].rolling(n, min_periods=1).max().shift(1)
    df['dn'] = df['low'].rolling(n, min_periods=1).min().shift(1)

    df['distance'] = 0

    condition1 = df['close'] > df['up']
    condition2 = df['close'] < df['dn']

    df.loc[condition1, 'distance'] =  1
    df.loc[condition2, 'distance'] = -1

    df[factor_name] = df['distance']

    return df