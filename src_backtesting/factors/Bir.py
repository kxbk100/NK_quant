#!/usr/bin/python3
# -*- coding: utf-8 -*-

import pandas as pd
import numpy  as np
from utils.diff import add_diff


# https://bbs.quantclass.cn/thread/18610

def signal(*args):
    df = args[0]
    n = args[1]
    diff_num = args[2]
    factor_name = args[3]

    df["p"] = (df["open"] + df["high"] + df["low"] + df["close"]) / 4.0
    df["p_max"] = df["p"].rolling(n, min_periods=1).max()
    df["p_min"] = df["p"].rolling(n, min_periods=1).min()
    short_period = max(n//3, 1)
    df["up"] = np.where(df["p"] > df["p_max"].shift(short_period), df["p"], df["p_max"].shift(short_period))
    df["up"] = (df["up"] - df["p_max"].shift(short_period)) / df["p_max"].shift(short_period)
    df["down"] = np.where(df["p"] < df["p_min"].shift(short_period), df["p"], df["p_min"].shift(short_period))
    df["down"] = (df["down"] - df["p_min"].shift(short_period)) / df["p_min"].shift(short_period)
    df[factor_name] = (df["up"] + df["down"]).rolling(short_period, min_periods=1).mean()

    del df["p"], df["p_max"], df["p_min"], df["up"], df["down"]

    if diff_num > 0:
        return add_diff(df, diff_num, factor_name)
    else:
        return df
