#!/usr/bin/python3
# -*- coding: utf-8 -*-
import pandas as pd
import numpy  as np
from utils.diff import add_diff


def signal(*args):
    # ZhenFu
    df = args[0]
    n = args[1]
    diff_num = args[2]
    factor_name = args[3]

    df['振幅'] = (df['high'] - df['low']) / df['open'] - 1
    df[factor_name] = df['振幅'].rolling(n).std(ddof=0)
    del df['振幅']

    if diff_num > 0:
        return add_diff(df, diff_num, factor_name)
    else:
        return df