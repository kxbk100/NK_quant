import numpy  as np
import pandas as pd
from utils.diff import add_diff


def signal(*args):
    # AvgPriceToLow
    # https://bbs.quantclass.cn/thread/9454

    df = args[0]
    n = args[1]
    diff_num = args[2]
    factor_name = args[3]

    df['price'] = df['quote_volume'].rolling(n, min_periods=1).sum() / df['volume'].rolling(n, min_periods=1).sum()
    df[factor_name] = df['price']/df['low'] - 1

    # 删除多余列
    del df['price']

    if diff_num > 0:
        return add_diff(df, diff_num, factor_name)
    else:
        return df