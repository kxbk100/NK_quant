#!/usr/bin/python3
# -*- coding: utf-8 -*-

import pandas as pd
import numpy  as np
import talib
from utils.diff import add_diff

#!/usr/bin/python3
# -*- coding: utf-8 -*-
import pandas as pd
import numpy  as np
import talib as ta
from utils.diff import add_diff, eps

# https://bbs.quantclass.cn/thread/18309

def signal(*args):
    '''
    AdaptBollingv3
    使用Sroc_v2 代替 mtm_mean 作为选B因子
    '''

    df, n, diff_num, factor_name = args

    n1 = int(n)

    # ==============================================================

    # df['mtm'] = df['close'] / df['close'].shift(n1) - 1
    # df['mtm_mean'] = df['mtm'].rolling(window=n1, min_periods=1).mean()

    ema = ta.KAMA(df['close'], n)
    ref = ema.shift(2 * n)
    df['sorc'] = (ema - ref) / (ref + eps)  

    # 基于价格atr，计算波动率因子wd_atr
    df['c1'] = df['high'] - df['low']
    df['c2'] = abs(df['high'] - df['close'].shift(1))
    df['c3'] = abs(df['low'] - df['close'].shift(1))
    df['tr'] = df[['c1', 'c2', 'c3']].max(axis=1)
    df['atr'] = df['tr'].rolling(window=n1, min_periods=1).mean()
    df['avg_price_'] = df['close'].rolling(window=n1, min_periods=1).mean()
    df['wd_atr'] = df['atr'] / df['avg_price_']

    # 参考ATR，对MTM指标，计算波动率因子
    df['mtm_l'] = df['low'] / df['low'].shift(n1) - 1
    df['mtm_h'] = df['high'] / df['high'].shift(n1) - 1
    df['mtm_c'] = df['close'] / df['close'].shift(n1) - 1
    df['mtm_c1'] = df['mtm_h'] - df['mtm_l']
    df['mtm_c2'] = abs(df['mtm_h'] - df['mtm_c'].shift(1))
    df['mtm_c3'] = abs(df['mtm_l'] - df['mtm_c'].shift(1))
    df['mtm_tr'] = df[['mtm_c1', 'mtm_c2', 'mtm_c3']].max(axis=1)
    df['mtm_atr'] = df['mtm_tr'].rolling(window=n1, min_periods=1).mean()

    # 参考ATR，对MTM mean指标，计算波动率因子
    df['mtm_l_mean'] = df['mtm_l'].rolling(window=n1, min_periods=1).mean()
    df['mtm_h_mean'] = df['mtm_h'].rolling(window=n1, min_periods=1).mean()
    df['mtm_c_mean'] = df['mtm_c'].rolling(window=n1, min_periods=1).mean()
    df['mtm_c1'] = df['mtm_h_mean'] - df['mtm_l_mean']
    df['mtm_c2'] = abs(df['mtm_h_mean'] - df['mtm_c_mean'].shift(1))
    df['mtm_c3'] = abs(df['mtm_l_mean'] - df['mtm_c_mean'].shift(1))
    df['mtm_tr'] = df[['mtm_c1', 'mtm_c2', 'mtm_c3']].max(axis=1)
    df['mtm_atr_mean'] = df['mtm_tr'].rolling(window=n1, min_periods=1).mean()

    indicator = 'sorc'

    # mtm_mean指标分别乘以三个波动率因子
    df[indicator] = df[indicator] * df['mtm_atr']
    df[indicator] = df[indicator] * df['mtm_atr_mean']
    df[indicator] = df[indicator] * df['wd_atr']

    df[factor_name] = df[indicator] * 100000000

    drop_col = [
        'c1', 'c2', 'c3', 'tr', 'atr', 'wd_atr', 'mtm_l',
        'mtm_h', 'mtm_c', 'mtm_c1', 'mtm_c2', 'mtm_c3', 'mtm_tr', 'mtm_atr',
        'mtm_l_mean', 'mtm_h_mean', 'mtm_c_mean', 'mtm_atr_mean', 'avg_price_'
    ]
    df.drop(columns=drop_col, inplace=True)

    if diff_num > 0:
        return add_diff(df, diff_num, factor_name)
    else:
        return df