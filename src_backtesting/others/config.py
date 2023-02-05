#!/usr/bin/python3
# -*- coding: utf-8 -*-

import os


_ = os.path.abspath(os.path.dirname(__file__))  # 返回当前文件路径
src_path = os.path.abspath(os.path.join(_, '..')) # 返回 src_backtesting
root_path = os.path.abspath(os.path.join(_, '..','..'))  # 返回根目录文件夹

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

