#!/usr/bin/python3
# -*- coding: utf-8 -*-
import pandas as pd
import numpy as np
import talib
from utils.diff import add_diff


def signal(*args):
    # 成交额使用收益率的sign进行加权求和，模拟资金净流出净流入的效果
    # https://bbs.quantclass.cn/thread/14374

    df = args[0]
    n  = args[1]
    diff_num = args[2]
    factor_name = args[3]

    df['_ret_sign'] = np.sign(df['close'].pct_change())
    df[factor_name] = (df['_ret_sign']*df['quote_volume']).rolling(n, min_periods=1).sum()

    del df['_ret_sign']

    if diff_num > 0:
        return add_diff(df, diff_num, factor_name)
    else:
        return df

