#!/usr/bin/python3
# -*- coding: utf-8 -*-

import os
import time
import pandas as pd
import numpy  as np
from multiprocessing import Pool
from datetime        import datetime, timedelta

from warnings import simplefilter
simplefilter(action='ignore', category=FutureWarning)
# SettingWithCopyWarning
pd.set_option('mode.chained_assignment', None)

import filter_handle
from api.market import fetch_fundingrate
from config import workdir, fundingrate_filename


def set_fundingrate(exchange, keep_hour=48):
	filepath = os.path.join(workdir, fundingrate_filename)
	recent = fetch_fundingrate(exchange)
	if not os.path.exists(filepath):
		recent.to_feather(filepath)
	else:
		df = pd.read_feather(filepath)
		df = df.append(recent)
		# ===先过滤可能重复的值
		df.sort_values(by=['candle_begin_time', 'symbol'], inplace=True)
		df.drop_duplicates(subset=['candle_begin_time', 'symbol'], inplace=True, keep='last') 
		# ===补全可能由于程序中间停掉的缺失值，资金费率给0
		curr_time  = datetime.now().replace(minute=0, second=0, microsecond=0)
		start_time = curr_time - timedelta(hours=keep_hour)
		st = start_time.strftime("%Y-%m-%d %H:%M:%S")
		et = curr_time.strftime("%Y-%m-%d %H:%M:%S")
		# 最大保留{keep_hour}数据
		df = df[df['candle_begin_time'] >= pd.to_datetime(st)]
		# 创建时间索引
		mux = pd.MultiIndex.from_product(
			[pd.date_range(start=st, end=et, freq='1H'), df['symbol'].unique()], 
			names=['candle_begin_time', 'symbol'])
		df = df.set_index(['candle_begin_time', 'symbol']).reindex(mux, fill_value=0).reset_index()
		df.sort_values(by=['candle_begin_time', 'symbol'], inplace=True)
		df.drop_duplicates(subset=['candle_begin_time', 'symbol'], inplace=True, keep='last') 
		df.reset_index(drop=True, inplace=True)
		df.to_feather(filepath)


def get_fundingrate():
	filepath = os.path.join(workdir, fundingrate_filename)
	if not os.path.exists(filepath):
		raise ValueError(f'资金费率文件{filepath}不存在!!!')
	return pd.read_feather(filepath)


def gen_selected(df, select_coin_num, long_weight, short_weight, before_handler, after_handler):
	df1 = df.copy()
	df2 = df.copy()
	# 前置过滤
	df1, df2 = getattr(filter_handle, before_handler)(df1, df2)

	# 根据因子对比进行排名
	# 从小到大排序
	df1['排名1'] = df1.groupby('candle_begin_time')['因子'].rank(method='first')
	df1 = df1[(df1['排名1'] <= select_coin_num)]
	df1['方向'] =  1 * long_weight

	# 从大到小排序
	df2['排名2'] = df2.groupby('candle_begin_time')['因子'].rank(method='first', ascending=False)
	df2 = df2[(df2['排名2'] <= select_coin_num)]
	df2['方向'] = -1 * short_weight

	# 后置过滤
	df1, df2 = getattr(filter_handle, after_handler)(df1, df2)

	# 合并排序结果
	df = pd.concat([df1, df2], ignore_index=True)

	df = df[['candle_begin_time', 'symbol', 'close', '方向']]
	df.sort_values(by=['candle_begin_time', '方向'], ascending=[True, False], inplace=True)
	df.reset_index(drop=True, inplace=True)

	return df


def convert_to_feature(factor_list):
	feature_list = set()
	for factor_name, if_reverse, back_hour, d_num, weight in factor_list:
		if d_num == 0:
			feature_list.add(f'{factor_name}_bh_{back_hour}')
		else:
			feature_list.add(f'{factor_name}_bh_{back_hour}_diff_{d_num}')

	return list(feature_list)


def convert_to_filter(filter_list):
	feature_list = set()
	for filter_, params in filter_list:
		feature_list.add('%s_fl_%s' % (filter_, str(params)))

	return list(feature_list)


# 纵截面
def cal_factor_by_vertical(df, factor_list, factor_tag='因子'):
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


# 横截面
def cal_factor_by_cross(df, factor_list, factor_tag='因子', pct_enable=False):
	feature_list = convert_to_feature(factor_list)
	# ===数据预处理
	df = df.set_index(['candle_begin_time', 'symbol']).sort_index()
	# 横截面排名
	df[feature_list] = df.groupby('candle_begin_time')[feature_list].apply(lambda x:x.rank(pct=pct_enable, ascending=True))
	df[feature_list] = df.groupby('candle_begin_time')[feature_list].apply(lambda x:x.fillna(x.median()))
	df.reset_index(inplace=True)

	return cal_factor_by_vertical(df, factor_list)


def cal_one_factors(df, all_factor_list, all_filter_list, run_time, hold_period):
	# 计算因子
	for factor, if_reverse, back_hour, d_num, weight in all_factor_list:
		if d_num == 0:
			_factor_name = f'{factor}_bh_{back_hour}'
		else:
			_factor_name = f'{factor}_bh_{back_hour}_diff_{d_num}'
		_cls = __import__('factors.%s' % factor,  fromlist=('', ))
		df   = getattr(_cls, 'signal')(df, int(back_hour), d_num, _factor_name)

	# 计算filter
	for filter_, params in all_filter_list:
		_filter_name = '%s_fl_%s' % (filter_, str(params))
		_cls = __import__('filters.%s' % filter_,  fromlist=('', ))
		df   = getattr(_cls, 'signal')(df, params, _filter_name)

	# ===时间过滤
	df = df[df['candle_begin_time'] >= (run_time - timedelta(hours=hold_period))]
	# ===只保留有用字段
	df = df[['candle_begin_time', 'symbol', 'close', 'fundingRate'] + convert_to_feature(all_factor_list) + convert_to_filter(all_filter_list)]

	return df


# =====策略相关函数
# 选币数据整理 & 选币
def cal_factor_and_select_coin(symbol_candle_data, strategy_list, run_time, njob2, min_kline_size=999):
	# ===过滤K线
	symbol_candle_datas = dict()
	for symbol, df in symbol_candle_data.items():
		if not symbol.endswith('USDT'):
			continue
		if df is None:
			continue
		if df.empty:
			print('no data', symbol)
			continue
		# 交易量为0不参与计算
		df = df[df['volume'] > 0].copy()
		# K先不足{min_kline_size}跟不参与计算
		if len(df) < min_kline_size:
			print('no enough data', symbol)
			continue

		from functions import get_fundingrate
		# 整合资金费率数据
		fundingrate_data = get_fundingrate()
		df = pd.merge(df,
					  fundingrate_data[['candle_begin_time', 'symbol', 'fundingRate']],
					  on=['candle_begin_time', 'symbol'], how="left")
		symbol_candle_datas[symbol] = df

	# ===因子计算(并行)
	# 构建参数
	all_factor_list = []
	all_filter_list = []
	for strategy in strategy_list:
		all_factor_list.extend(strategy['factors'])
		all_filter_list.extend(strategy['filters'])
	# 最大hold_period
	# 计算所有因子首先通过 df['candle_begin_time'] >= (run_time - timedelta(hours=hold_period)) 过滤
	# 主要减少下面 pd.concat 内存消耗
	max_hold_period = max([int(row['hold_period'][:-1]) for row in strategy_list])

	arg_list = [(df, all_factor_list, all_filter_list, run_time, max_hold_period) for df in symbol_candle_datas.values()]
	# 计算
	if njob2 == 1:
		period_df_list = []
		for df, all_factor_list, all_filter_list, run_time, max_hold_period in arg_list:
			df_ = cal_one_factors(df, all_factor_list, all_filter_list, run_time, max_hold_period)
			if df_ is not None and not df_.empty:
				period_df_list.append(df_)
	else:
		with Pool(processes=njob2) as pool:
			period_df_list = pool.starmap(cal_one_factors, arg_list)
	# ===合并所有K线
	alldata = pd.concat(period_df_list, ignore_index=True)
	alldata.sort_values(by=['candle_begin_time', 'symbol'], inplace=True)
	alldata.reset_index(drop=True, inplace=True)
	# ===数据预处理
	alldata = alldata.set_index(['candle_begin_time', 'symbol']).sort_index()
	alldata = alldata.replace([np.inf, -np.inf], np.nan)
	# 因子空值都用中位数填充, 如果填0可能后面rank排序在第一或者最后
	#alldata = alldata.fillna(value=0).reset_index()
	_feature_list = convert_to_feature(all_factor_list)
	alldata[_feature_list] = alldata[_feature_list].apply(lambda x:x.fillna(x.median()))
	alldata = alldata.reset_index()
	
	# ===计算信号
	select_coin_list = []
	for strategy in strategy_list:
		c_factor        = strategy['c_factor']
		hold_hour       = strategy['hold_period']
		type_ 		    = strategy['type']
		factor_list     = strategy['factors']
		filter_list     = strategy['filters']
		before_handler  = strategy['filters_handle']['before']
		after_handler   = strategy['filters_handle']['after']
		long_weight     = strategy['long_weight'] 
		short_weight    = strategy['short_weight'] 
		select_coin_num = strategy['select_coin_num'] 
		if type_ not in ('横截面', '纵截面'): raise ValueError(f'{type_} 类型不对')

		# ===计算因子
		if type_ == '横截面':
			df = cal_factor_by_cross(alldata.copy(), factor_list)
		else:
			df = cal_factor_by_vertical(alldata.copy(), factor_list)
		# ===时间过滤
		df = df[df['candle_begin_time'] >= (run_time - timedelta(hours=int(hold_hour[:-1])))]
		# ===只保留有用字段
		df = df[['candle_begin_time', 'symbol', 'close', '因子', 'fundingRate'] + convert_to_filter(filter_list)]

		# ===选币
		df = gen_selected(df, select_coin_num, long_weight, short_weight, before_handler, after_handler)
		# ===处理字段
		#df['symbol'] = df['symbol'].apply(lambda x: x.upper())
		#df['offset'] = df['candle_begin_time'].apply(lambda x: x.to_pydatetime().hour%int(hold_hour[:-1]))
		#df['key']    = df['candle_begin_time'].apply(lambda x: f'{c_factor}_{hold_hour}_' + str(x.to_pydatetime().hour%int(hold_hour[:-1])) + 'H')
		df['offset'] = df['candle_begin_time'].apply(lambda x: int(((x.to_pydatetime() - pd.to_datetime('2017-01-01')).total_seconds()/3600)%int(hold_hour[:-1])))
		df['key']    = df['candle_begin_time'].apply(lambda x: f'{c_factor}_{hold_hour}_' + str(int(((x.to_pydatetime() - pd.to_datetime('2017-01-01')).total_seconds()/3600)%int(hold_hour[:-1]))) + 'H')

		select_coin_list.append(df)
	# ===合并选币结果
	select_coin = pd.concat(select_coin_list)
	return select_coin






