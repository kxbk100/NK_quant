#!/usr/bin/python3
# -*- coding: utf-8 -*-

import os


_ = os.path.abspath(os.path.dirname(__file__))  # 返回当前文件路径
root_path = os.path.abspath(os.path.join(_, '..'))  # 返回根目录文件夹
# 创建目录
pickle_path = os.path.join(root_path, 'data', 'pickle_data')
if not os.path.exists(pickle_path):
	os.mkdir(pickle_path)
data_path = os.path.join(root_path, 'data', 'factors')
if not os.path.exists(data_path):
	os.mkdir(data_path)
output_path = os.path.join(_, 'output')
if not os.path.exists(output_path):
	os.mkdir(output_path)
# 资金费率目录
fundingrate_path = os.path.join(root_path, 'data', 'funding_rate')


head_columns = [
	'candle_begin_time', 
	'symbol', 
	'open',
	'high',
	'low',
	'close',
	'avg_price',
	'下个周期_avg_price', 
	'volume',                  
]  


factor_class_list  = ['Bias', 'Cci', 'KdjD','Dbcd','VixBw','Rccd','ZhenFu_v2']

'''
factor_class_list  = ['Bias', 'Cci', 'Adtm', 'Adx', 'Angle', 'Atr',
    'AvgPrice', 'Cmo', 'Dbcd', 'Erbear', 'Erbull',
    'Force', 'Gap', 'KdjD', 'KdjJ', 'KdjK', 'MadisPlaced',
    'MagicCci', 'Mfi', 'Obv', 'Pmo', 'Pos', 'Psy',
    'QuanlityPriceCorr', 'QuoteVolumeRatio', 'Rccd',
    'Reg', 'Rsi', 'Stc', 'TakerByRatio', 'TradeNum',
    'Trix', 'Vidya', 'VixBw', 'Volume', 'VolumeStd',
    'Vramt', 'Vwap', 'Vwapbias', 'ZhangDieFu',
    'ZhangDieFuSkew', 'ZhangDieFuStd', 'ZhenFu_v2',
    'ZhenFu']
'''

filter_config_list = [
	{
		'filter': 'AdaptBolling',
		'params_list': [60, 100]
	},
	{
		'filter': 'AdaptBollingv3',
		'params_list': [11, 12, 13, 14]
	},
	{
		'filter': 'BBW',
		'params_list': [[20, 2], ]
	},
	{
		'filter': 'DC',
		'params_list': [20, 40, 60, 80, 100]
	},	
	{
		'filter': 'ZH_涨跌幅',
		'params_list': [4, 6, 8, 12, 24]
	},	
	{
		'filter': 'ZH_震幅',
		'params_list': [4, 6, 8, 12, 24]
	},	
]
