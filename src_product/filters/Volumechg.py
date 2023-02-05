#!/usr/bin/python3
# -*- coding: utf-8 -*-
import pandas as pd
import numpy as np
from utils.diff import add_diff

# https://bbs.quantclass.cn/thread/18247

def signal(*args):
    df = args[0]
    n = args[1]
    factor_name = args[2]

    df['该小时涨跌幅'] = df['close'].pct_change(1)
    df.loc[df['该小时涨跌幅'] > 0, '方向'] = 1
    df.loc[df['该小时涨跌幅'] < 0, '方向'] = -1
    df['成交量变化'] = df['quote_volume'] / df['quote_volume'].shift(1) * df['方向']
    df[factor_name] = df['成交量变化'].rolling(n).max()

    return df
