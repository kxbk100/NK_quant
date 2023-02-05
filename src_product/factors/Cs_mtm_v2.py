#!/usr/bin/python3
# -*- coding: utf-8 -*-
from utils.diff import add_diff, eps

# https://bbs.quantclass.cn/thread/18743

def signal(*args):
    # Cs_mtm_v2 指标
    df = args[0]
    n  = args[1]
    diff_num = args[2]
    factor_name = args[3]
    # 收盘价动量
    df['c_mtm'] = df['close'] / df['close'].shift(n) - 1
    df['c_mtm'] = df['c_mtm'].rolling(n, min_periods=1).mean()
    # 标准差动量
    df['std'] = df['close'].rolling(n, min_periods=1).std(ddof=0)
    df['s_mtm'] = df['std'] / df['std'].shift(n)
    df['s_mtm'] = df['s_mtm'].rolling(n, min_periods=1).mean()
    # 成交量变化
    df['v_mtm'] = df['quote_volume'] / df['quote_volume'].shift(n)
    df['v_mtm'] = df['v_mtm'].rolling(n, min_periods=1).mean()
    df[factor_name] = df['c_mtm'] * df['s_mtm'] * df['v_mtm']

    del df['c_mtm'], df['std'], df['s_mtm'], df['v_mtm']

    if diff_num > 0:
        return add_diff(df, diff_num, factor_name)
    else:
        return df