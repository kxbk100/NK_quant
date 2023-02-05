import numpy  as np
import pandas as pd
from utils.diff import add_diff


def signal(*args):
    # Mak
    # https://bbs.quantclass.cn/thread/9446

    df = args[0]
    n = 15
    diff_num = args[2]
    factor_name = args[3]

    df['ma'] = df['close'].rolling(n, min_periods=1).mean()
    df[factor_name] = (df['ma'] / df['ma'].shift(1) - 1) * 1000  # 原涨跌幅值太小，所以乘以1000放大一下。

    del df['ma']

    if diff_num > 0:
        return add_diff(df, diff_num, factor_name)
    else:
        return df