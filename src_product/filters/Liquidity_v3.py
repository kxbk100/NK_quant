#!/usr/bin/python3
# -*- coding: utf-8 -*-
import pandas as pd
import numpy as np
from utils.diff import add_diff

# https://bbs.quantclass.cn/thread/18774

def signal(*args):
    """
    流动性因子，源自2023年分享会直播01期的选股策略；
    成交额/|价格变化|，即价格变化（上涨或下跌）1个单位，需要多少交易额；
    因子值越小，流动性越差——每个单位波动需要的资金少；
    """

    df, n, factor_name = args
    candels_num = int(n)

    # 价格运行路径1：开->高->低->收
    df['path_first'] = (df['high'] - df['open']) + (df['high'] - df['low']) + (df['close'] - df['low'])
    # 价格运行路径2：开->低->高->收
    df['path_second'] = (df['open'] - df['low']) + (df['high'] - df['low']) + (df['high'] - df['close'])

    df['path_min'] = df.loc[:, ['path_first', 'path_second']].min(axis=1)
    df['change'] = df['high'] - df['low']  # 最短路径如果为0，使用此价差替代
    df['path_min'] = np.where(df['path_min'] == 0, df['change'], df['path_min'])
    df['path_min'] = df['path_min'] + abs(df['open']-df['close'].shift(1))  # 跳空高（低）开

    # 最短路径归一化
    df['path_shortest'] = df['path_min'] / df['close']

    df[factor_name] = np.where(df['path_shortest'] == 0, 0, df['quote_volume'] / df['path_shortest'])
    df[factor_name] = df[factor_name].rolling(candels_num, min_periods=1).sum()  # or mean

    del df['path_first'], df['path_second'], df['path_min'], df['change'], df['path_shortest']

    return df