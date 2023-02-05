import numpy  as np
import pandas as pd
from utils.diff import add_diff

# https://bbs.quantclass.cn/thread/18782
# https://bbs.quantclass.cn/thread/9446

def signal(*args):
    df = args[0]
    n = args[1]
    diff_num = args[2]
    factor_name = args[3]

    df['ma'] = df['close'].rolling(n, min_periods=1).mean()
    df['Mak'] = (df['ma'] / df['ma'].shift(1) - 1) * 1000
    df[factor_name] = df['Mak'].rolling(n, min_periods=1).mean()

    del df['ma'], df['Mak']

    if diff_num > 0:
        return add_diff(df, diff_num, factor_name)
    else:
        return df