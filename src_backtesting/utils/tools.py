#!/usr/bin/python3
# -*- coding: utf-8 -*-

import os
import pandas as pd
import numpy  as np
import matplotlib.pyplot as plt


def convert_to_filter_cls(fliter_list):
	cls_list = set()
	for f in fliter_list:
		cls_list.add(f.split('_fl_')[0])

	return list(cls_list)


def convert_to_cls(factor_list):
	cls_list = set()
	for factor_name, if_reverse, back_hour, d_num, weight in factor_list:
		cls_list.add(factor_name)

	return list(cls_list)


def convert_to_feature(factor_list):
	feature_list = set()
	for factor_name, if_reverse, back_hour, d_num, weight in factor_list:
	    if d_num == 0:
	        feature_list.add(f'{factor_name}_bh_{back_hour}')
	    else:
	        feature_list.add(f'{factor_name}_bh_{back_hour}_diff_{d_num}')

	return list(feature_list)


# 横截面
def cal_factor_by_cross(df, factor_list, factor_tag='因子', pct_enable=False):
    feature_list = convert_to_feature(factor_list)
    # ===数据预处理
    df = df.set_index(['candle_begin_time', 'symbol']).sort_index()
    # 横截面排名
    df[feature_list] = df.groupby('candle_begin_time')[feature_list].apply(lambda x:x.rank(pct=pct_enable, ascending=True))
    df[feature_list] = df.groupby('candle_begin_time')[feature_list].apply(lambda x:x.fillna(x.median()))
    df.reset_index(inplace=True)

    return cal_factor_by_verical(df, factor_list, factor_tag=factor_tag)


# 纵截面
def cal_factor_by_verical(df, factor_list, factor_tag='因子'):
	feature_list = []
	coef_        = []
	for factor_name, if_reverse, back_hour, d_num, weight in factor_list:
		reverse_ = -1 if if_reverse else 1
		if d_num == 0:
			_factor = f'{factor_name}_bh_{back_hour}'
		else:
			_factor = f'{factor_name}_bh_{back_hour}_diff_{d_num}'
		feature_list.append(_factor)
		coef_.append(weight * reverse_)
	coef_ = pd.Series(coef_, index=feature_list)
	df[f'{factor_tag}'] = df[feature_list].dot(coef_.T)
	return df


def evaluate(df, c_rate, select_coin_num):
    df['本周期涨跌幅'] = -(1 * c_rate) + 1 * (1 + df['ret_next'] * df['方向']) * (1 - c_rate) - 1
    select_coin = pd.DataFrame()
    select_coin['本周期多空涨跌幅'] = df.groupby('candle_begin_time')['本周期涨跌幅'].sum()/(select_coin_num*2)
    select_coin.reset_index(inplace=True)
    return select_coin


def plot(select_c, mdd_std=0.2):
	plt.rcParams['axes.unicode_minus'] = False      # 用来正常显示负号
	plt.figure(figsize=(12, 6), dpi=80)
	plt.figure(1)

	condition = (select_c['dd2here'] >= -mdd_std) & (select_c['dd2here'].shift(1) < -mdd_std)
	select_c[f'回撤上穿{mdd_std}次数'] = 0
	select_c.loc[condition, f'回撤上穿{mdd_std}次数'] = 1
	mdd_num = int(select_c[f'回撤上穿{mdd_std}次数'].sum())
	ax = plt.subplot(2, 1, 1)

	plt.subplots_adjust(hspace=1)  # 调整子图间距
	plt.title(f'Back draw{mdd_std} Number: {mdd_num}', fontsize='large', fontweight = 'bold',color='blue', loc='center')  # 设置字体大小与格式
	ax.plot(select_c['candle_begin_time'], select_c['资金曲线'])
	ax2 = ax.twinx() # 设置y轴次轴
	ax2.plot(select_c["candle_begin_time"], -select_c['dd2here'], color='red', alpha=0.4)
	plt.show()


	
