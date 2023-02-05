#!/usr/bin/python3
# -*- coding: utf-8 -*-

import numpy  as np
import pandas as pd
from utils.diff import add_diff

# https://bbs.quantclass.cn/thread/18979

def signal(*args):
    df, n, diff_num, factor_name = args

    # 时序标准化
    cr = df['close'].rolling(n, min_periods=1)
    close_standard = (df['close'] - cr.min()) / (cr.max() - cr.min())
    # 指数平均
    df[factor_name] = close_standard.ewm(span=n - 1, min_periods=1, adjust=False).mean()

    if diff_num > 0:
        return add_diff(df, diff_num, factor_name)
    else:
        return df