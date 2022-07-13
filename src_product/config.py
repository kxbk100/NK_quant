#!/usr/bin/python3
# -*- coding: utf-8 -*-

import os
import math
import ccxt

from utils.commons import robust
import platform


apiKey = ''
secret = ''
# 是否开启调试模式
Debug = False
proxy = {}
# 如果使用代理 注意替换IP和Port
# proxy  = {"http":  "http://127.0.0.1:9910", "https": "http://127.0.0.1:9910"}
# 并行取K线进程数
njob1  = 1
# 并行计算因子进程数
njob2  = 1
# 总资金杠杆数
trade_ratio = 1
# 最小可用K线数(如果不足该币种不参与交易)
min_kline_size = 999
# 币种黑名单(不参与交易)
black_list = ['BTCDOMUSDT', ]
# ===拆单配置
# 每次最大下单金额
max_one_order_amount = 800
# 拆单后暂停时间(单位: 秒)
twap_interval = 2
# 数据存储路径
workdir = './data'
# 资金费率文件名
fundingrate_filename = 'fundingRate.pkl'
# ===策略配置
stratagy_list = [
	{
		"c_factor":    "c_factor1",  
		"hold_period": "6H",
		"type":        "横截面",
		"factors": [
			('Bias', False, 4, 0, 1.0),('Cci', True, 36, 0, 0.3)
		],
		"filters": [
			('AdaptBolling', 100), ('BBW', [20, 2])
		],
		"filters_handle": {
			"before": 'filter_before',
			'after':  'default_handler',
		},
		"long_weight":       1,
		"short_weight":      1,
		"select_coin_num":   1,
	},
	{
		"c_factor":    "c_factor2",  
		"hold_period": "6H",
		"type":        "横截面",
		"factors": [
			('Bias', False, 4, 0, 1.0),('Cci', True, 36, 0, 0.3)
		],
		"filters": [],
		"filters_handle": {
			"before": 'default_handler',
			'after':  'filter_fundingrate',
		},
		"long_weight":       1,
		"short_weight":      1,
		"select_coin_num":   1,
	},
	{
		"c_factor":    "百因子",  
		"hold_period": "6H",
		"type":        "横截面",
		"factors": [
			('Bias', True, 12, 0, 0.5), ('Cci', True, 4, 0, -0.1), ('Cci', True, 8, 0, 0.4), ('Bias', True, 4, 0, 0.3), ('Bias', True, 48, 0, -0.5), ('Cci', True, 30, 0, -0.4), ('Bias', True, 36, 0, 0.1), ('Bias', True, 30, 0, 0.6), ('Cci', True, 96, 0, -0.9), ('Cci', True, 9, 0, -1.0), ('Cci', True, 48, 0, -0.8), ('Bias', True, 72, 0, -0.1), ('Bias', True, 96, 0, 0.8), ('Cci', True, 24, 0, 0.2), ('Bias', True, 3, 0, 0.8), ('Cci', True, 36, 0, -0.6), ('Cci', True, 3, 0, 0.1), ('Bias', True, 60, 0, 0.1), ('Cci', True, 60, 0, -0.6), ('Bias', True, 9, 0, 0.1), ('Bias', True, 6, 0, 1.0), ('Cci', True, 72, 0, -0.1), ('Bias', True, 8, 0, -0.4), ('Cci', True, 12, 0, 0.9), ('Bias', True, 24, 0, 0.1), 
		],
		"filters": [],
		"filters_handle": {
			"before": 'default_handler',
			'after':  'default_handler',
		},
		"long_weight":       1,
		"short_weight":      1,
		"select_coin_num":   1,
	},
	{
		"c_factor":    "lasso",  
		"hold_period": "6H",
		"type":        "纵截面",
		"factors": [
			('Bias', True, 30, 0, -17.12139161127422),('Bias', True, 36, 0, -7.319543605225477),('Bias', True, 4, 0, -0.46805207271163196),('Cci', True, 3, 0, -0.00015315921056340784),('Bias', True, 96, 0, 0.7177159150564656),('Bias', True, 3, 0, 0.6117244602628059),('Cci', True, 24, 0, 0.0002809635974661574),('Cci', True, 4, 0, -2.659624052634533e-05),('Bias', True, 24, 0, 20.29683359255328),('Cci', True, 48, 0, 0.0012869610841340507),('Cci', True, 8, 0, 0.00010777199718734396),('Cci', True, 30, 0, 0.0008542498996673277),('Bias', True, 8, 0, 0.5588303494746705),('Cci', True, 72, 0, 0.0011219536745510285),('Bias', True, 12, 0, 0.07952677978466707),('Bias', True, 72, 0, 0.7797845059481301),('Cci', True, 60, 0, -0.0017813577134868283),('Cci', True, 6, 0, 0.00016532912957839486),('Cci', True, 36, 0, -0.0008257809766224287),('Cci', True, 12, 0, -0.0005635837663922271),('Cci', True, 96, 0, 2.0745292410454156e-05),('Bias', True, 9, 0, -7.339379836799855),('Bias', True, 48, 0, 13.762453913157907),('Cci', True, 9, 0, -0.00015514286805485221),('Bias', True, 60, 0, -7.978598711402175),
		],
		"filters": [],
		"filters_handle": {
			"before": 'default_handler',
			'after':  'default_handler',
		},
		"long_weight":       1,
		"short_weight":      1,
		"select_coin_num":   1,
	},

]

class QuantConfig:
	def __init__(self, apiKey, secret, proxy, njob1, njob2, trade_ratio, min_kline_size, black_list,
			max_one_order_amount, twap_interval, stratagy_list, debug=False):
		self._initialize    	  = False
		self.apiKey 	    	  = apiKey
		self.secret 	    	  = secret
		self.proxy 		    	  = proxy
		self.njob1 		    	  = njob1
		self.njob2 		    	  = njob2
		self.trade_ratio          = trade_ratio
		self.min_kline_size 	  = min_kline_size
		self.black_list 	      = black_list
		self.max_one_order_amount = max_one_order_amount
		self.twap_interval 		  = twap_interval
		self.stratagy_list  	  = stratagy_list
		self.debug 		    	  = debug

	def _init_exchange(self):
		self.exchange = ccxt.binance({
			'apiKey':    self.apiKey,
			'secret':    self.secret,
			'timeout':   30000,
			'rateLimit': 10,
			'enableRateLimit': False,
			'options': {
			    'adjustForTimeDifference': True,  # ←---- resolves the timestamp
			    'recvWindow': 10000,
			},
		})
		self.exchange.proxies = self.proxy

	def initialize(self):
		if not self._initialize:
			self._init_exchange()
			# 初始化存储路径
			if not os.path.exists(workdir):
				os.makedirs(workdir)
			self._initialize = True

	def load_market(self):
		exchange = self.exchange
		# 获取账户净值
		exchange_info = robust(exchange.fapiPublic_get_exchangeinfo, func_name='fapiPublic_get_exchangeinfo')  
		_symbol_list  = [x['symbol'] for x in exchange_info['symbols']]
		symbol_list   = [symbol for symbol in _symbol_list if symbol.endswith('USDT')]

		_temp_list = []
		for symbol in symbol_list:
			if symbol in ['COCOSUSDT', 'BTCSTUSDT', 'DREPUSDT', 'SUNUSDT']:
				continue
			if symbol.endswith(('DOWNUSDT', 'UPUSDT', 'BULLUSDT', 'BEARUSDT')):
				continue
			# 处理黑名单
			if symbol in self.black_list:
				continue
			_temp_list.append(symbol)
		self.symbol_list = _temp_list

		min_qty = {}
		price_precision = {}
		min_notional = {}

		for x in exchange_info['symbols']:
			_symbol = x['symbol']

			for _filter in x['filters']:
				if _filter['filterType'] == 'PRICE_FILTER':
					price_precision[_symbol] = int(math.log(float(_filter['tickSize']), 0.1))
				elif _filter['filterType'] == 'LOT_SIZE':
					min_qty[_symbol] = int(math.log(float(_filter['minQty']), 0.1))
				elif _filter['filterType'] == 'MIN_NOTIONAL':
					min_notional[_symbol] = float(_filter['notional'])

		self.min_qty = min_qty
		self.price_precision = price_precision
		self.min_notional = min_notional


quant = QuantConfig(apiKey, secret, proxy, njob1, njob2, trade_ratio, min_kline_size, black_list,
	max_one_order_amount, twap_interval, stratagy_list, debug=Debug)

if platform.system() != 'Linux' and (quant.njob1 != 1 or quant.njob2 != 1):
	quant._init_exchange()

