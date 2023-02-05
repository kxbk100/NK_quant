#!/usr/bin/python3
# -*- coding: utf-8 -*-

import os
import time
import glob
import itertools
import pandas as pd
import numpy  as np
from datetime import datetime, timedelta
import matplotlib.pyplot as plt

pd.set_option('expand_frame_repr', False)
pd.set_option('display.max_rows',  1000)  # 最多显示数据的行数
# setcopywarning
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


#==================================================================
#==================================================================
#==================================================================
#==================================================================
# 前置过滤
def filter_before(df1, df2):
	filter_factor = '涨跌幅max_fl_24'
	df1 = df1[(df1[filter_factor] <= 0.2)]
	df2 = df2[(df2[filter_factor] <= 0.2)]

	return df1, df2


# 后置过滤
def filter_after(df1, df2):
	# 资金费率为负不做空
	#df2 = df2[df2['fundingRate']>=0]

	return df1, df2


def gen_selected(df, select_coin_num):
	df1 = df.copy()
	df2 = df.copy()
	# 前置过滤
	df1, df2 = filter_before(df1, df2)

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
	# 后置过滤
	df1, df2 = filter_after(df1, df2)

	# 合并排序结果
	df = pd.concat([df1, df2], ignore_index=True)

	df = df[header_columns + ['方向']]
	df.sort_values(by=['candle_begin_time', '方向'], ascending=[True, False], inplace=True)
	df.reset_index(drop=True, inplace=True)

	return df

#==================================================================
#==================================================================
#==================================================================
#==================================================================


start_date = '2021-01-01'
end_date   = '2022-11-20'

header_columns  = ['candle_begin_time', 'symbol', 'ret_next']
select_coin_num = 1
c_rate 			= 6/10000
trade_type 		= 'swap'
hold_hour  		= '12H'
filter_list     = [
	'涨跌幅max_fl_24'
]
factor_list     = [
	('AdaptBollingv3', True, 96, 0, 0.5)
]


factor_class_list = tools.convert_to_cls(factor_list)
filter_class_list = tools.convert_to_filter_cls(filter_list)
feature_list      = tools.convert_to_feature(factor_list)

# ===读取数据
df = reader.readall(trade_type, hold_hour, factor_class_list, filter_class_list=filter_class_list, date_range=(start_date, end_date))
# 6.0.5 以上版本，请用下面一段话
df = reader.feature_shift(df, feature_list+filter_list)

# 删除某些行数据
df = df[df['volume'] > 0]  # 该周期不交易的币种
df.dropna(subset=['下个周期_avg_price'], inplace=True)  # 最后几行数据，下个周期_avg_price为空
# 筛选日期范围
df = df[df['candle_begin_time'] >= pd.to_datetime(start_date)]
df = df[df['candle_begin_time'] <= pd.to_datetime(end_date)]
# ===数据预处理
df['ret_next'] = df['下个周期_avg_price'] / df['avg_price'] - 1
df = df[header_columns + feature_list + filter_list]
df = df.set_index(['candle_begin_time', 'symbol']).sort_index()
df = df.replace([np.inf, -np.inf], np.nan)
# 因子空值都用中位数填充, 如果填0可能后面rank排序在第一或者最后
#df = df.fillna(value=0).reset_index()
df[feature_list] = df[feature_list].apply(lambda x:x.fillna(x.median()))
df = df.reset_index()
print('数据处理完毕!!!\n')

'''
# ===整合资金费率
fundingrate_data = reader.read_fundingrate()
df = pd.merge(df, 
	fundingrate_data[['candle_begin_time', 'symbol', 'fundingRate']], 
	on=['candle_begin_time', 'symbol'], how="left")
df['fundingRate'].fillna(value=0, inplace=True)
print('整合资金费率完毕!!!\n')
'''

# ===计算因子
# 横截面
df = tools.cal_factor_by_cross(df, factor_list)
# 纵截面
df = tools.cal_factor_by_vertical(df, factor_list)
# ===选币
select_coin = gen_selected(df, select_coin_num)
# ===计算offset
#select_coin['offset'] = select_coin['candle_begin_time'].apply(lambda x: x.to_pydatetime().hour%int(hold_hour[:-1]))
select_coin['offset'] = select_coin['candle_begin_time'].apply(lambda x: int(((x.to_pydatetime() - pd.to_datetime('2017-01-01')).total_seconds()/3600)%int(hold_hour[:-1])))


temp = pd.DataFrame()

# ===显示所有offset资金曲线
for offset, g_df in select_coin.groupby('offset'):
	g_df.sort_values(by='candle_begin_time', inplace=True)
	g_df.reset_index(drop=True, inplace=True)
	# ===计算涨跌幅
	select_c = tools.evaluate(g_df, c_rate, select_coin_num)	
	# ===计算资金曲线
	select_c['资金曲线'] = (select_c['本周期多空涨跌幅'] + 1).cumprod()
	select_c['offset'] = offset
	# select_c.to_csv(f'select_c_{offset}.csv')

	rtn, select_c = ind.cal_ind(select_c)
	print(rtn)
	temp = temp.append(rtn, ignore_index=True)
	ax = plt.subplot(int(hold_hour[:-1]), 1, offset + 1)
	ax.plot(select_c['candle_begin_time'], select_c['资金曲线'])

print(temp.to_markdown())
plt.gcf().autofmt_xdate()
plt.show()
# exit()

# """
# ===计算涨跌幅
all_select_df = tools.evaluate(select_coin, c_rate, select_coin_num)
# ===合并资金曲线
all_select_df['本周期多空涨跌幅'] = all_select_df['本周期多空涨跌幅']/int(hold_hour[:-1])
all_select_df['资金曲线'] = (all_select_df['本周期多空涨跌幅'] + 1).cumprod()
print(all_select_df)

rtn, select_c = ind.cal_ind(all_select_df)
temp = temp.append(rtn)
print('所有offset平均"年化收益/回撤比" ', temp['年化收益/回撤比'][:-1].mean())
# print('所有offset平均"年化收益/回撤比(合成)" ', temp['年化收益/回撤比'].iloc[-1])
print(temp.to_markdown())
# print(rtn)
print('\n')
tools.plot(select_c, mdd_std=0.2)
# """
