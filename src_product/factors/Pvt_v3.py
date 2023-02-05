#!/usr/bin/python3
# -*- coding: utf-8 -*-

import numpy  as np
import pandas as pd
import talib as ta
from utils.diff import add_diff, eps


def signal(*args):
    # Pvt_v3 指标
    df = args[0]
    n = args[1]
    diff_num = args[2]
    factor_name = args[3]

    '''
    PVT=(CLOSE-REF(CLOSE,1))/REF(CLOSE,1)*VOLUME 
    PVT_MA1=MA(PVT,N1)
    PVT_MA2=MA(PVT,N2)
    PVT 指标用价格的变化率作为权重求成交量的移动平均。
    PVT 指标 与 OBV 指标的思想类似，但与 OBV 指标相比，
    PVT 考虑了价格不同涨跌幅的影响，而 OBV 只考虑了价格的变化方向。
    我们这里用 PVT 短期和长期均线的交叉来产生交易信号。
    如果 PVT_MA1 上穿 PVT_MA2，则产生买入信号; 
    如果 PVT_MA1 下穿 PVT_MA2，则产生卖出信号。
    '''

    df['PVT'] = df['close'].pct_change() * df['volume']
    # 去量纲
    df['PVT_score'] = ((df['PVT'] - df['PVT'].rolling(n, min_periods=1).mean())
                       / (df['PVT'].rolling(n, min_periods=1).std() + eps))
    df['PVT_sum'] = df['PVT_score'].rolling(n, min_periods=1).sum()
    pvt_ma1 = df['PVT_sum'].rolling(n, min_periods=1).mean()
    pvt_ma2 = df['PVT_sum'].rolling(2 * n, min_periods=1).mean()
    df[factor_name] = pd.Series(pvt_ma1 - pvt_ma2)

    # 删除无关列
    del df['PVT'], df['PVT_score'], df['PVT_sum']

    if diff_num > 0:
        return add_diff(df, diff_num, factor_name)
    else:
        return df
