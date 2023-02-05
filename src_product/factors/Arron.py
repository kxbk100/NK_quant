#!/usr/bin/python3
# -*- coding: utf-8 -*-

import pandas as pd
import numpy  as np
import numba as nb
from utils.diff import add_diff


# =====函数  zscore归一化
def scale_zscore(_s, _n):
    _s = (pd.Series(_s) - pd.Series(_s).rolling(_n, min_periods=1).mean()
          ) / pd.Series(_s).rolling(_n, min_periods=1).std()
    return pd.Series(_s)


@nb.njit(nb.int32[:](nb.float64[:], nb.int32), cache=True)
def rolling_argmin_queue(arr, n):
    results = np.empty(len(arr), dtype=np.int32)

    head = 0
    tail = 0
    que_idx = np.empty(len(arr), dtype=np.int32)
    for i, x in enumerate(arr[:n]):
        while tail > 0 and arr[que_idx[tail - 1]] > x:
            tail -= 1
        que_idx[tail] = i
        tail += 1
        results[i] = que_idx[0]

    for i, x in enumerate(arr[n:], n):
        if que_idx[head] <= i - n:
            head += 1
        while tail > head and arr[que_idx[tail - 1]] > x:
            tail -= 1
        que_idx[tail] = i
        tail += 1
        results[i] = que_idx[head]
    return results


def signal(*args):
    df = args[0]
    n = args[1]
    diff_num = args[2]
    factor_name = args[3]
    # ******************** Arron ********************
    # ArronUp = (N - HIGH_LEN) / N * 100
    # ArronDown = (N - LOW_LEN) / N * 100
    # ArronOs = ArronUp - ArronDown
    # 其中 HIGH_LEN，LOW_LEN 分别为过去N天最高/最低价距离当前日的天数
    # ArronUp、ArronDown指标分别为考虑的时间段内最高价、最低价出现时间与当前时间的距离占时间段长度的百分比。
    # 如果价格当天创新高，则ArronUp等于100；创新低，则ArronDown等于100。Aroon指标为两者之差，
    # 变化范围为-100到100。Arron指标大于0表示股价呈上升趋势，Arron指标小于0表示股价呈下降趋势。
    # 距离0点越远则趋势越强。我们这里以20/-20为阈值构造交易信号。如果ArronOs上穿20/下穿-20则产生买入/卖出信号。
    low_len = (rolling_argmin_queue(df['low'].values, n))
    high_len = (rolling_argmin_queue(-df['high'].values, n))
    signal =  pd.Series((high_len - low_len) * 100 / n)
    df[factor_name] = scale_zscore(signal, n)

    if diff_num > 0:
        return add_diff(df, diff_num, factor_name)
    else:
        return df