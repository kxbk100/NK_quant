import numpy  as np
import pandas as pd
import talib as ta
from utils.diff import add_diff, eps


def signal(*args):
    # ZhenZhangRatio 振幅 涨幅的比率
    # https://bbs.quantclass.cn/thread/9454

    df = args[0]
    n = args[1]
    diff_num = args[2]
    factor_name = args[3]

    # 涨跌幅std，振幅的另外一种形式
    df['Zhang'] = df['close'].pct_change()
    df['zhen'] = df['high']/df['low'] - 1
    df[factor_name] = df['zhen']/(df['Zhang'] + eps)

    if diff_num > 0:
        return add_diff(df, diff_num, factor_name)
    else:
        return df