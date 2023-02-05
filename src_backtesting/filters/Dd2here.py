#!/usr/bin/python3
# -*- coding: utf-8 -*-

import pandas as pd
import numpy  as np

# https://bbs.quantclass.cn/thread/18105

def signal(*args):
    """
    Dd2here旨在构建一套突破回撤体系，通过最大回撤超过多少拉黑n小时
    """
    df = args[0]
    n = args[1]
    factor_name = args[2]

    df['max2here'] = df['high'].rolling(n, min_periods=1).max()
    df['dd1here'] = abs(df['close'] / df['max2here'] - 1)
    # df['avg_max_drawdown'] = df['dd1here'].rolling(n, min_periods=1).mean()

    df['min2here'] = df['low'].rolling(n, min_periods=1).min()
    df['dd2here'] = abs(df['close'] / df['min2here'] - 1)
    # df['avg_reverse_drawdown'] = df['dd2here'].rolling(n, min_periods=1).mean()

    df[factor_name] = df[['dd1here', 'dd2here']].min(axis=1).rolling(n, min_periods=1).max()
    drop_col = [
        'max2here', 'dd1here', 'min2here', 'dd2here'
    ]
    df.drop(columns=drop_col, inplace=True)

    return df
