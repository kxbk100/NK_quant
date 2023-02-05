import pandas as pd
import numpy  as np
from utils.diff import add_diff, eps


#wma(加权移动平均线)
def wma(df, column='close', k=10):
    weights = np.arange(1, k + 1)
    wmas = df[column].rolling(k).apply(lambda x: np.dot(x, weights) / weights.sum(), raw=True).to_list()
    return wmas

#sma(简单移动平均线)
def sma(df, column='close', k=10):
    smas = df[column].rolling(k, min_periods=1).mean()
    return smas

#ema（指數平滑移動平均線）备用
def ema(df, column='close', k=10):
    emas = df[column].ewm(k, adjust=False).mean()
    return emas

# 指标名 版本： FearGreed_Yidai_v1
# https://bbs.quantclass.cn/thread/9458
def signal(*args):
    df = args[0]
    n = args[1]
    diff_num = args[2]
    factor_name = args[3]

    #计算TR 真实振幅 并作平滑 标准化（后续计算采用标准化参数）
    df['c1'] = df['high'] - df['low']  # HIGH-LOW
    df['c2'] = abs(df['high'] - df['close'].shift(1))  # ABS(HIGH-REF(CLOSE,1)
    df['c3'] = abs(df['low'] - df['close'].shift(1))  # ABS(LOW-REF(CLOSE,1))
    df['TR'] = df[['c1', 'c2', 'c3']].max(axis=1)
    df['sma'] = sma(df, column='close', k=n)
    df['STR'] = df['TR']/df['sma']

    # 多空振幅分离
    df['trUp'] = np.where(df['close'] > df['close'].shift(1), df['STR'], 0)
    df['trDn'] = np.where(df['close'] < df['close'].shift(1), df['STR'], 0)

    # 多空振幅平滑 快慢均线
    df['wmatrUp1'] = wma(df, column='trUp', k=n)
    df['wmatrDn1'] = wma(df, column='trDn', k=n)
    df['wmatrUp2'] = wma(df, column='trUp', k=2*n)
    df['wmatrDn2'] = wma(df, column='trDn', k=2*n)

    # 多空振幅比较 1阶导 描绘速度 并作平滑
    df['fastDiff'] = df['wmatrUp1'] - df['wmatrDn1']
    df['slowDiff'] = df['wmatrUp2'] - df['wmatrDn2']

    # 快慢均线比较描绘 2阶导 描绘加速度
    df['FastMinusSlow'] = df['fastDiff'] - df['slowDiff']
    df['fgi'] = wma(df, column='FastMinusSlow', k=n)

    # 返回df
    df[factor_name] = df['fgi']

    # 删除多余列
    del df['c1'], df['c2'], df['c3'], df['TR'], df['STR'], df['sma']
    del df['trUp'], df['trDn'], df['fastDiff'], df['slowDiff'], df['FastMinusSlow'], df['fgi']
    del df['wmatrUp1'], df['wmatrDn1'], df['wmatrUp2'], df['wmatrDn2']

    if diff_num > 0:
        return add_diff(df, diff_num, factor_name)
    else:
        return df