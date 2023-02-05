#!/usr/bin/python3
# -*- coding: utf-8 -*-

import pandas as pd
import numpy  as np
from utils.diff import add_diff

# https://bbs.quantclass.cn/thread/18643

def signal(*args):
    '''
    BiasCubic指标    三个bias相乘
    要把变量尽可能减少  back_hour_list  = [3, 4, 6, 8, 9, 12, 24, 30, 36, 48, 60, 72, 96]
    int(n / 2) 取个整数，除以1.5，乘以1.5来进行三个区分也是可以的    实现1个变量当三个用。
    :param args:
    :return:
    '''

    df = args[0]
    n = args[1]
    diff_num = args[2]
    factor_name = args[3]

    df['ma_1'] = df['close'].rolling(int(n / 2), min_periods=1).mean()
    df['ma_2'] = df['close'].rolling(n, min_periods=1).mean()
    df['ma_3'] = df['close'].rolling(n * 2, min_periods=1).mean()
    df['bias_1'] = (df['close'] / df['ma_1'] - 1)
    df['bias_2'] = (df['close'] / df['ma_2'] - 1)
    df['bias_3'] = (df['close'] / df['ma_3'] - 1)


    df['mtm'] = (df['bias_1'] * df['bias_2'] *df['bias_3'])* df['quote_volume']/df['quote_volume'].rolling(n, min_periods=1).mean()
    df[factor_name] = df['mtm'].rolling(n, min_periods=1).mean()

    del df['ma_1'], df['ma_2'], df['ma_3'], df['bias_1'], df['bias_2'], df['bias_3'],df['mtm']

    if diff_num > 0:
        return add_diff(df, diff_num, factor_name)
    else:
        return df