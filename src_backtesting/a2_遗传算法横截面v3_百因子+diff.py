#!/usr/bin/python3
# -*- coding: utf-8 -*-

import os
import time
import pandas as pd
import numpy  as np
import geatpy as ea
from datetime import datetime, timedelta
from multiprocessing import Pool as ProcessPool
import traceback
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

from utils import reader, tools
from utils.target import target_ratio as target_


#==================================================================
#==================================================================
#==================================================================
#==================================================================
# 前置过滤
def filter_before(df1, df2):
	"""
	filter_factor = 'AdaptBolling_fl_100'
	# 破下轨不做多
	df1 = df1[~(df1[filter_factor] == -1)]
	# 破上轨不做空
	df2 = df2[~(df2[filter_factor] ==  1)]

	filter_factor = 'ZH_震幅_fl_4'
	df1 = df1[(abs(df1[filter_factor]) <= 0.5)]
	df2 = df2[(abs(df2[filter_factor]) <= 0.5)]

	filter_factor = 'ZH_涨跌幅_fl_4'
	df1 = df1[(df1[filter_factor] <=  0.4)]
	df2 = df2[(df2[filter_factor] >= -0.4)]
	"""

	return df1, df2


# 后置过滤
def filter_after(df1, df2):
	return df1, df2


def gen_selected(df, select_coin_num):
	df1 = df.copy()
	df2 = df.copy()
	# 前置过滤
	df1, df2 = filter_before(df1, df2)

	# 根据因子对比进行排名
	# 从小到大排序
	df1['排名1'] = df1.groupby('candle_begin_time')['因子'].rank(method='first')
	df1 = df1[(df1['排名1'] <= select_coin_num)]
	df1['方向'] = 1

	# 从大到小排序
	df2['排名2'] = df2.groupby('candle_begin_time')['因子'].rank(method='first', ascending=False)
	df2 = df2[(df2['排名2'] <= select_coin_num)]
	df2['方向'] = -1

	df1, df2 = filter_after(df1, df2)

	del df2['排名2']
	del df1['排名1']
	# 合并排序结果
	df = pd.concat([df1, df2], ignore_index=True)

	df = df[['candle_begin_time', 'symbol', 'ret_next', '方向']]
	df.sort_values(by=['candle_begin_time', '方向'], ascending=[True, False], inplace=True)
	df.reset_index(drop=True, inplace=True)

	return df


def cal_factor_by_verical(df, factor_list, reverse=-1, factor_tag='因子'):
	feature_list = []
	coef_        = []
	for _factor, weight in factor_list:
		feature_list.append(_factor)
		coef_.append(weight * reverse)
	coef_ = pd.Series(coef_, index=feature_list)
	df[f'{factor_tag}'] = df[feature_list].dot(coef_.T)
	return df


def evaluate(header, features, filters, hold_hour, all_factor_list):
	def check_zero(factor_list):
		zero_counter = 0
		for factor_, weight in factor_list:
			if weight == 0:
				zero_counter += 1
		if zero_counter == len(factor_list):
			return True
		return False

	# =====计算目标结果
	# check zero
	if check_zero(all_factor_list):
		return 0, all_factor_list
	# 计算因子
	_reverse = -1 if if_reverse == True else 1
	features = cal_factor_by_verical(features, all_factor_list, reverse=_reverse)
	# 组装df
	df = header
	df['因子'] = features['因子']
	for f in filters.columns:
		df[f] = filters[f]
	# 选币
	df = gen_selected(df, select_coin_num)
	# 计算offset
	df['offset'] = df['candle_begin_time'].apply(lambda x: int(((x.to_pydatetime() - pd.to_datetime('2017-01-01')).total_seconds()/3600)%int(hold_hour[:-1])))

	# 计算目标函数
	net = 0
	for offset, g_df in df.groupby('offset'):
		g_df.sort_values(by='candle_begin_time', inplace=True)
		g_df.reset_index(drop=True, inplace=True)
		# ===计算涨跌幅
		select_c = tools.evaluate(g_df, c_rate, select_coin_num)	
		# ===计算资金曲线
		select_c['资金曲线'] = (select_c['本周期多空涨跌幅'] + 1).cumprod()
		# ===计算目标函数
		net_ = target_(select_c)
		if np.isnan(net_):
			net_ = 0
		net += net_

	return net/int(hold_hour[:-1]), all_factor_list


class alphaFactory(ea.Problem):  # 继承Problem父类
	def __init__(self, Dim, lb, ub, lbin, ubin, alldata, hold_hour, feature_list):
		name 	  = 'alphaFactory'  # 初始化name（函数名称，可以随意设置）
		M 		  = 1  				# 初始化M（目标维数）
		maxormins = [-1]  			# 初始化maxormins（目标最小最大化标记列表，1：最小化该目标；-1：最大化该目标）
		Dim_ 	  = Dim  			# 初始化Dim（决策变量维数）
		varTypes  = [1] * Dim_  	# 初始化varTypes（决策变量的类型，元素为0表示对应的变量是连续的；1表示是离散的）

		# 决策矩阵
		lb_   	  = lb  			# 决策变量下界
		ub_   	  = ub  			# 决策变量上界
		lbin_ 	  = lbin  			# 决策变量下边界（0表示不包含该变量的下边界，1表示包含）
		ubin_ 	  = ubin  			# 决策变量上边界（0表示不包含该变量的上边界，1表示包含）
		# 调用父类构造方法完成实例化
		ea.Problem.__init__(self, name, M, maxormins, Dim_, varTypes, lb_, ub_, lbin_, ubin_)
		self.pool       = ProcessPool(njobs)  # 设置池的大小
		self.best_score = 0
		self.cache  	= None
		# 自定义参数
		self.alldata      = alldata
		self.hold_hour    = hold_hour
		self.feature_list = feature_list

	def aimFunc(self, pop):  # 目标函数
		Vars = pop.Phen  # 得到决策变量矩阵

		args = []
		for i in range(NIND):
			all_factor_list = []
			for j in range(len(self.feature_list)):
				factor     = self.feature_list[j]
				weight 	   = int(Vars[:, [j]][i]) * 0.1
				all_factor_list.append((factor, weight))

			header_df  = self.alldata[header_columns]
			feature_df = self.alldata[feature_list].copy()
			filter_df  = self.alldata[filter_list]
			args.append([header_df, feature_df, filter_df, self.hold_hour, all_factor_list])

		result_list = self.pool.starmap(evaluate, args)

		target_list = []
		for target, factor_list in result_list:
			if target > self.best_score:
				self.best_score = target
				self.cache      = factor_list
				# 跟踪打印最优结果
				show_r = []
				for factor, weight in self.cache:
					factor_name = factor.split('_bh_')[0]
					if "_diff_" in factor:
						d_num   = float(factor.split('_diff_')[1])
						back_hour   = int(factor.split('_diff_')[0].split('_bh_')[1])
					else:
						d_num = 0
						back_hour   = int(factor.split('_bh_')[1])
					if weight != 0:
						show_r.append((factor_name, if_reverse, back_hour, d_num, round(weight, 1)))
				line = 'best_score: ' + str(self.best_score)
				line += '\n    '
				for _f, _reverse_num, back_hour, d_num, weight in show_r:
					_reverse = 'False' if _reverse_num == 0 else 'True'
					line += f"('{_f}', {_reverse}, {back_hour}, {d_num}, {weight})"
					line += ', '
				with open(result_dir, 'a+') as f:
					f.write(line)
					f.write('\n')

				print('best_score', self.best_score, show_r)
			target_list.append(target)

		pop.ObjV = np.array([target_list]).transpose()   # 累积净值

	def calReferObjV(self):  # 设定目标数参考值（本问题目标函数参考值设定为理论最优值）
		referenceObjV = np.array([[10000]])
		return referenceObjV

#==================================================================
#==================================================================
#==================================================================
#==================================================================
result_dir = './output/geatv3.txt'
start_date = '2020-06-01'
end_date   = '2021-02-01'

NIND  = 10 	   	# 种群规模
njobs = 10

if_reverse = True
pct_enable = False
header_columns  = ['candle_begin_time', 'symbol', 'ret_next']
select_coin_num = 1
c_rate 			= 6/10000
trade_type 		= 'swap'
hold_hour  		= '6H'      # 持币周期
back_hour_list  = [3, 4, 6, 8, 9, 12, 24, 30, 36, 48, 60, 72, 96]
diff_list=[0, 0.3, 0.5, 0.9]
filter_list     = [
	# 'AdaptBolling_fl_100',
	# 'ZH_涨跌幅_fl_4',
	# 'ZH_震幅_fl_4',
]
factor_classes  = [
	'Bias', 
	'Cci', 
]
# ====过滤重复因子
factors = set()
for _factor_name in factor_classes:
	factors.add(_factor_name)
factors = list(factors)

all_factor_list = []
for factor_name in factors:
	for back_hour in back_hour_list:
		for d_num in diff_list:
			all_factor_list.append((factor_name, True, back_hour, d_num, 1.0))

filter_class_list = tools.convert_to_filter_cls(filter_list)
feature_list      = tools.convert_to_feature(all_factor_list)


if __name__ == '__main__':
	if os.path.exists(result_dir):
		os.remove(result_dir)

	# ===读取数据
	df = reader.readall(trade_type, hold_hour, factor_classes, filter_class_list=filter_class_list, date_range=(start_date, end_date))
	df = reader.feature_shift(df, feature_list + filter_list)

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
	#df = df.fillna(value=0)

	df[feature_list] = df[feature_list].apply(lambda x:x.fillna(x.median()))
	# 横截面排名
	df[feature_list] = df.groupby('candle_begin_time')[feature_list].apply(lambda x:x.rank(pct=pct_enable, ascending=True))
	df[feature_list] = df.groupby('candle_begin_time')[feature_list].apply(lambda x:x.fillna(x.median()))
	df.reset_index(inplace=True)
	print('数据处理完毕!!!\n')

	from utils import mem_usage
	df = mem_usage.reduce_mem_usage(df, feature_list)
	print('数据压缩完毕!!!\n')


	# ===定义决策矩阵
	lb   = [-10] * len(feature_list)
	ub   = [10]  * len(feature_list)
	lbin = [1]   * len(feature_list)
	ubin = [1]   * len(feature_list)
	Dim  = len(feature_list)

	"""================================实例化问题对象==========================="""
	problem = alphaFactory(Dim, lb, ub, lbin, ubin, df, hold_hour, feature_list)  # 生成问题对象
	"""==================================种群设置=============================="""
	Encoding   = 'RI'  # 编码方式
	Field      = ea.crtfld(Encoding, problem.varTypes, problem.ranges, problem.borders)  # 创建区域描述器
	population = ea.Population(Encoding, Field, NIND)  # 实例化种群对象（此时种群还没被初始化，仅仅是完成种群对象的实例化）
	"""================================算法参数设置============================="""
	myAlgorithm = ea.soea_DE_rand_1_bin_templet(problem, population)  # 实例化一个算法模板对象
	myAlgorithm.MAXGEN       	= 1500	  	# 1000 最大进化代数
	myAlgorithm.mutOper.F    	= 0.5  		# 差分进化中的参数F 0.5
	myAlgorithm.recOper.XOVR 	= 0.7  		# 重组概率 0.7
	myAlgorithm.logTras      	= 1  		# 设置每隔多少代记录日志，若设置成0则表示不记录日志
	myAlgorithm.verbose      	= True  	# 设置是否打印输出日志信息
	myAlgorithm.drawing      	= 0 		# 设置绘图方式（0：不绘图；1：绘制结果图；2：绘制目标空间过程动画；3：绘制决策空间过程动画）
	#myAlgorithm.trappedValue 	 = 1e-6  	# “进化停滞”判断阈值
	#myAlgorithm.maxTrappedCount = 50  		# 进化停滞计数器最大上限值，如果连续maxTrappedCount代被判定进化陷入停滞，则终止进化
	"""===========================调用算法模板进行种群进化========================"""
	try:
		[BestIndi, population] = myAlgorithm.run()  # 执行算法模板，得到最优个体以及最后一代种群
		#BestIndi.save()  # 把最优个体的信息保存到文件中
	except Exception as reason:
		traceback.print_exc()
		print(problem.best_score)
		print(problem.cache)
		exit()
	"""==================================输出结果=============================="""
	print('评价次数：%s' % myAlgorithm.evalsNum)
	print('时间已过 %s 秒' % myAlgorithm.passTime)








