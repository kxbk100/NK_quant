#!/usr/bin/python3
# -*- coding: utf-8 -*-

import pandas as pd
import numpy  as np
from utils.diff import add_diff


def signal(*args):
    # Bias
    df = args[0]
    n = args[1]
    diff_num = args[2]
    factor_name = args[3]

    df['ma'] = df['close'].rolling(n, min_periods=1).mean()
    df['mtm'] = (df['close'] / df['ma'] - 1) * df['quote_volume']/df['quote_volume'].rolling(n, min_periods=1).mean()
    # EMA
    df[factor_name] = df['mtm'].ewm(n, adjust=False).mean()

    del df['ma'], df['mtm']

    if diff_num > 0:
        return add_diff(df, diff_num, factor_name)
    else:
        return df