import numpy  as np
import pandas as pd
import talib as ta
from utils.diff import add_diff


def signal(*args):
    # Mreg
    # https://bbs.quantclass.cn/thread/9840

    df = args[0]
    n = args[1]
    diff_num = args[2]
    factor_name = args[3]



    df['reg_close'] = ta.LINEARREG(df['close'], timeperiod=n)  # 该部分为talib内置求线性回归
    df['mreg'] = df['close'] / df['reg_close'] - 1
    df[factor_name] = df['mreg'].rolling(n, min_periods=1).mean()



    # 删除多余列
    del df['reg_close'], df['mreg']



    if diff_num > 0:
        return add_diff(df, diff_num, factor_name)
    else:
        return df