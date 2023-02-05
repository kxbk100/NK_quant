#!/usr/bin/python3
# -*- coding: utf-8 -*-

import os
import time
import pandas as pd
import numpy  as np
from glob     import glob
from joblib   import Parallel, delayed
from datetime import datetime, timedelta
pd.set_option('expand_frame_repr', False)
pd.set_option('display.max_rows',  1000)  # 最多显示数据的行数
# SettingWithCopyWarning
pd.set_option('mode.chained_assignment', None)
# UserWarning
import warnings
warnings.filterwarnings("ignore")
# FutureWarning
from warnings import simplefilter
simplefilter(action='ignore', category=FutureWarning)
from config import src_path
import sys
sys.path.append(src_path)

from utils import reader, tools, ind


def gen_selected(df, select_coin_num):
	df1 = df.copy()
	df2 = df.copy()

	# 根据因子对比进行排名
	# 从小到大排序
	df1['排名1'] = df1.groupby('candle_begin_time')['因子'].rank(method='first')
	df1 = df1[(df1['排名1'] <= select_coin_num)].copy()
	df1['方向'] = 1

	# 从大到小排序
	df2['排名2'] = df2.groupby('candle_begin_time')['因子'].rank(method='first', ascending=False)
	df2 = df2[(df2['排名2'] <= select_coin_num)].copy()
	df2['方向'] = -1

	del df2['排名2']
	del df1['排名1']

	# 合并排序结果
	df = pd.concat([df1, df2], ignore_index=True)

	df = df[['candle_begin_time', 'symbol', 'ret_next', '方向']]
	df.sort_values(by=['candle_begin_time', '方向'], ascending=[True, False], inplace=True)
	df.reset_index(drop=True, inplace=True)

	return df


def cal_one_factor(df, feature, reverse):
	#print(feature, reverse)
	df_ = df[head_columns + [feature]].copy()
	df_['因子'] = -1 * df_[feature] if reverse else df_[feature]
	select_coin = gen_selected(df_, select_coin_num)
	select_coin['offset'] = select_coin['candle_begin_time'].apply(lambda x: x.to_pydatetime().hour%int(hold_hour[:-1]))

	results = []
	for offset, g_df in select_coin.groupby('offset'):
		g_df = g_df.copy()
		select_df = tools.evaluate(g_df, c_rate, select_coin_num)
		select_df['资金曲线'] = (select_df['本周期多空涨跌幅'] + 1).cumprod()
		rtn, select_c = ind.cal_ind(select_df)

		ind1 = rtn['累积净值'].values[0]
		ind2 = rtn['最大回撤'].values[0]
		ind3 = rtn['胜率'].values[0]
		ind4 = rtn['盈亏收益比'].values[0]
		ind5 = rtn['最大连续盈利周期数'].values[0]
		ind6 = rtn['最大连续亏损周期数'].values[0]
		ind7 = rtn['最大回撤开始时间'].values[0]
		ind8 = rtn['最大回撤结束时间'].values[0]
		ind9 = rtn['年化收益/回撤比'].values[0]
		results.append([offset, feature, reverse, ind1, ind2, ind3, ind4, ind5, ind6, ind7, ind8, ind9])
	return results


# 参数设定
start_date = '2021-01-01'
end_date   = '2022-01-01'

trade_type      = 'swap'
hold_hour       = '6H'
select_coin_num = 1
c_rate 			= 6/10000
reverse_list    = [True, False]
back_hour_list  = [3, 4, 6, 8, 9, 12, 24, 30, 36, 48, 60, 72, 96] 
# 不使用差分 diff_list = [0, ]	 			
diff_list 		= [0, 0.3, 0.5] 
factor_classes  = [
	'Bias', 
	'Cci', 
]
all_factor_list = []
for factor_name in factor_classes:
	for back_hour in back_hour_list:
		for d_num in diff_list:
			all_factor_list.append([factor_name, True, back_hour, d_num, 1.0])

all_feature_list = tools.convert_to_feature(all_factor_list)
head_columns = ['candle_begin_time', 'symbol', 'ret_next']
# ===读取数据
df = reader.readall(trade_type, hold_hour, factor_classes, date_range=(start_date, end_date))
df = reader.feature_shift(df, all_feature_list)
# 删除某些行数据
df = df[df['volume'] > 0]  # 该周期不交易的币种
df.dropna(subset=['下个周期_avg_price'], inplace=True)  # 最后几行数据，下个周期_avg_price为空
# 筛选日期范围
df = df[df['candle_begin_time'] >= pd.to_datetime(start_date)]
df = df[df['candle_begin_time'] <= pd.to_datetime(end_date)]
# ===数据预处理
df['ret_next'] = df['下个周期_avg_price'] / df['avg_price'] - 1
df = df[head_columns + all_feature_list]
df = df.set_index(['candle_begin_time', 'symbol']).sort_index()
df = df.replace([np.inf, -np.inf], np.nan)
# 因子空值都用中位数填充, 如果填0可能后面rank排序在第一或者最后
#df = df.fillna(value=0).reset_index()
df[all_feature_list] = df[all_feature_list].apply(lambda x:x.fillna(x.median()))
df = df.reset_index()
print('数据处理完毕!!!\n')

"""
alldatas = []
for feature in all_feature_list:
	for reverse in reverse_list:
		results = cal_one_factor(df, feature, reverse)
		alldatas.extend(results)
"""

alldatas = Parallel(n_jobs=16)(
	delayed(cal_one_factor)(df, feature, reverse)
		for feature in all_feature_list
		for reverse in reverse_list
)
# 展平list
rtn_list = []
for rows in alldatas:
    for row in rows:
        rtn_list.append(row)

rtn = pd.DataFrame(rtn_list, 
	columns=['offset', '因子名称', '是否反转', 
		'累积净值', '最大回撤', '胜率', '盈亏收益比', 
		'最大连盈', '最大连亏', '最大回撤开始时间', 
		'最大回撤结束时间', '年化收益/回撤比']
)
rtn.sort_values(by=['年化收益/回撤比'], inplace=True, ascending=False)
rtn.reset_index(drop=True, inplace=True)
print(rtn)





