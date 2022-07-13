#!/usr/bin/python3
# -*- coding: utf-8 -*-

import os

import warnings
warnings.filterwarnings("ignore")

import pandas as pd
import numpy  as np


def preprocess_by_median(df, feature_list):
	# ===对因子进行预处理
	# 对于因子中的 nan 和 inf，用该期的中位数填充，如果该期所有因子都为 nan，
	# 则用上一期的中位数填充，这样该因子该期处于中间位置，在后面的排序中不会有什么贡献
	median = df.groupby(['candle_begin_time', 'symbol'])[feature_list].median().ffill()
	tmp    = df.join(median, on=['candle_begin_time', 'symbol'], rsuffix='_median')
	for f in feature_list:
	    df[f].fillna(tmp[f'{f}_median'], inplace=True)
	del tmp
	print('null rows:', df.isna().any(axis=1).sum())
	print('\n')

	return df


# 百分位去极值 中位数填充同时处理函数
def extreme_process_quantile(data, feature_list, down=0.05, up=0.95):
    data_ = data.copy()  # 为不破坏原始数据，先对其进行拷贝
    min_thres = data_.quantile(q=down)
    max_thres = data_.quantile(q=up)
    data_.loc[:,feature_list]=data_.loc[:,feature_list].clip(lower=min_thres, upper=max_thres, axis=1) #利用clip()函数，将因子取值限定在上下限范围内，即用上下限来代替异常值
    data_ = data_.replace([np.inf, -np.inf], np.nan)
    data_[feature_list] = data_[feature_list].apply(lambda factor: factor.fillna(factor.median()))
    return data_


# Standardization
def data_scale_Z_Score(data, feature_list):
    data_ = data.copy()  # 为不破坏原始数据，先对其进行拷贝
    data_.loc[:,feature_list] = (data_.loc[:,feature_list]-data_.loc[:,feature_list].mean())/data_.loc[:,feature_list].std()
    return data_



    