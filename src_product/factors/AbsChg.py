#!/usr/bin/python3
# -*- coding: utf-8 -*-

import numpy  as np
import pandas as pd
from utils.diff import add_diff


#abschg
def signal(*args):
    # https://bbs.quantclass.cn/thread/9776

    df = args[0]
    n = args[1]
    diff_num = args[2]
    factor_name = args[3]

    df[factor_name] = abs(df['close'].pct_change(16))

    if diff_num > 0:
        return add_diff(df, diff_num, factor_name)
    else:
        return df