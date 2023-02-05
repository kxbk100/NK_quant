#!/usr/bin/python3
# -*- coding: utf-8 -*-

import numpy  as np
import pandas as pd
from utils.diff import add_diff, eps

# https://bbs.quantclass.cn/thread/18772

def signal(*args):
    df = args[0]
    n = args[1]
    diff_num = args[2]
    factor_name = args[3]

    df['pc'] = df['close'].pct_change()
    df['vol'] = df['pc'].rolling(n).std()
    df['ret'] = df['pc'].rolling(n).sum()
    df['cvr'] = (df['ret']/(df['vol'] + eps)) * (df['quote_volume']/df['quote_volume'].rolling(n, min_periods=1).mean())
    df[factor_name] = df['cvr'].rolling(n, min_periods=1).mean()
    df.drop(columns = ['pc', 'vol', 'ret', 'cvr'], inplace=True)
    if diff_num > 0:
        return add_diff(df, diff_num, factor_name)
    else:
        return df