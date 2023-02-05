import pandas as pd
import numpy as np
from utils.diff import add_diff


def signal(*args):
    # https://bbs.quantclass.cn/thread/14038
    # 一种衡量量价的指标 描述价格的突破难度
    # 一定的价格变动幅度是由于量的变动引起的，如果一定的价格变动需要更多的量，可以说明该标的更难受到控制
    df = args[0]
    n = args[1]
    diff_num = args[2]
    factor_name = args[3]

    df["close_shift"] = df["close"].shift(n)
    df["volume_shift"] = df["volume"].shift(n)
    df["close_ratio"] = abs((df["close"] - df["close_shift"].rolling(n).mean()) / df["close_shift"])
    df["volume_ratio"] = (df["volume"] - df["volume_shift"].rolling(n).mean()) / df["volume_shift"]

    df["angle"] = df["close_ratio"] * df["volume_ratio"]

    condition = df["angle"] < 0  # 量价方向不同,突破毫不费力，设定为inf
    df["direction"] = 1
    df["adj"] = 1
    df.loc[condition, 'direction'] = -1  # 用来把指标改为正数
    df.loc[condition, 'adj'] = np.inf

    df[factor_name] = df["close_ratio"] / df["volume_ratio"] * df["direction"] * df["adj"]
    df[factor_name] = df[factor_name] / n  # 时间窗口越长，拉平指标

    del df['close_shift']
    del df['volume_shift']
    del df['close_ratio']
    del df['volume_ratio']
    del df['angle']
    del df['direction']
    del df['adj']

    if diff_num > 0:
        return add_diff(df, diff_num, factor_name)
    else:
        return df