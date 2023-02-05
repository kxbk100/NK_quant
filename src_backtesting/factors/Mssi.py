import numpy  as np
import pandas as pd
from utils.diff import add_diff


def signal(*args):
    """
    https://bbs.quantclass.cn/thread/9501
    取一段时间内的平均最大回撤和平均最大反向回撤中的最大值构成市场情绪平稳度指数
    Market Sentiment Stability Index
    指标越小，说明趋势性越强
    """

    df = args[0]
    n  = args[1]
    diff_num = args[2]
    factor_name = args[3]

    df['max2here'] = df['high'].rolling(n, min_periods=1).max()
    df['dd1here'] = abs(df['close']/df['max2here'] - 1)
    df['avg_max_drawdown'] = df['dd1here'].rolling(n, min_periods=1).mean()

    df['min2here'] = df['low'].rolling(n, min_periods=1).min()
    df['dd2here'] = abs(df['close'] / df['min2here'] - 1)
    df['avg_reverse_drawdown'] = df['dd2here'].rolling(n, min_periods=1).mean()

    df[factor_name] = df[['avg_max_drawdown', 'avg_reverse_drawdown']].max(axis=1)

    del df['max2here']
    del df['dd1here']
    del df['avg_max_drawdown']
    del df['min2here']
    del df['dd2here']
    del df['avg_reverse_drawdown']

    if diff_num > 0:
        return add_diff(df, diff_num, factor_name)
    else:
        return df