import pandas as pd
import numpy  as np
from utils.diff import add_diff, eps


# 因子 指标名 版本： Pmarp_Yidai_v1
# https://bbs.quantclass.cn/thread/9501
def signal(*args):
    df = args[0]
    n = args[1]
    diff_num = args[2]
    factor_name = args[3]


    # 计算sma
    df['sma'] = df['close'].rolling(n, min_periods=1).mean()

    # 当前价格与sma比较（百分比）：价格对移动均线的相对涨跌
    df['pmar'] = abs(df['close']/df['sma'])

    # 计算当前k线某一特征值超过了统计范围内多少根k线？返回百分比
    # 统计 当前k线的pmar 超过了统计周期内 多少根k线的pmar 返回值
    df['pmarpSum'] = 0

    k = n
    while k > 0:
        df['pmardiff'] = df['pmar'] - df['pmar'].shift(k)
        df['add'] = np.where(df['pmardiff'] > 0, 1, 0)
        df['pmarpSum'] = df['pmarpSum'] + df['add']
        k -= 1

    df[factor_name] = df['pmarpSum'] / n * 100


    # 删除多余列
    del df['sma'], df['pmar'], df['pmardiff'], df['add'], df['pmarpSum']

    if diff_num > 0:
        return add_diff(df, diff_num, factor_name)
    else:
        return df