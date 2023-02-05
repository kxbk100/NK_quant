import pandas as pd
import numpy as np
import talib as ta
from utils.diff import add_diff, eps


def signal(*args):
    # Sroc_v2
    # https://bbs.quantclass.cn/thread/9807

    df = args[0]
    n = args[1]
    diff_num = args[2]
    factor_name = args[3]

    ema = ta.KAMA(df['close'], n)
    ref = ema.shift(2 * n)
    df[factor_name] = (ema - ref) / (ref + eps)

    if diff_num > 0:
        return add_diff(df, diff_num, factor_name)
    else:
        return df