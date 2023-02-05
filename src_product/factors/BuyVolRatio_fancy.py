#!/usr/bin/python3
# -*- coding: utf-8 -*-
import pandas as pd
import numpy as np
import talib
from utils.diff import add_diff


def signal(*args):
    # 过去N分钟的主买比例
    # https://bbs.quantclass.cn/thread/14374

    df = args[0]
    n  = args[1]
    diff_num = args[2]
    factor_name = args[3]

    df[factor_name] = df['taker_buy_quote_asset_volume'].rolling(n, min_periods=1).sum() / df['quote_volume'].rolling(n, min_periods=1).sum()

    if diff_num > 0:
        return add_diff(df, diff_num, factor_name)
    else:
        return df