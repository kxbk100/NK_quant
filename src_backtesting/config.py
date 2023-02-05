#!/usr/bin/python3
# -*- coding: utf-8 -*-

import os


_ = os.path.abspath(os.path.dirname(__file__))  # 返回当前文件路径
plackback_path = os.path.abspath(os.path.join(_, 'plackback'))  # 返回plackback文件夹
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


factor_class_list  = ['AdaptBollingv3']

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
		'filter': '涨跌幅max',
		'params_list': [4, 6, 8, 12, 24]
	},
	{
		'filter': 'Volume',
		'params_list': [3, 6, 12, 24, 36, 48, 72]
	},
]
