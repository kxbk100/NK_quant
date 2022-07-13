#!/usr/bin/python3
# -*- coding: utf-8 -*-

import os
import sys
import time
import math
import pandas as pd
import numpy  as np
from datetime import datetime, timedelta
import traceback

from api.asset     import cal_strategy_trade_usdt
from api.position  import update_symbol_info, reset_leverage
from api.market    import fetch_all_binance_swap_candle_data, fetch_binance_ticker_data
from api.trade     import cal_order_amount, place_order, get_twap_symbol_info_list

from functions     import cal_factor_and_select_coin, set_fundingrate
from utils.commons import robust, sleep_until_run_time
from rebalance.fee import replenish_bnb
from config        import quant


def run():
	# ====初始化
	quant.initialize()
	debug = quant.debug
	if not debug:
		# ===设置默认最大杠杆数，变相增加资金容量
		reset_leverage(quant.exchange, max_leverage=5)

	while True:
		# ===加载市场信息
		quant.load_market()
		# =====设置参数
		exchange        	 = quant.exchange
		symbol_list     	 = quant.symbol_list
		min_qty         	 = quant.min_qty
		price_precision 	 = quant.price_precision
		min_notional  		 = quant.min_notional
		njob1           	 = quant.njob1
		njob2           	 = quant.njob2
		trade_ratio     	 = quant.trade_ratio
		stratagy_list   	 = quant.stratagy_list
		min_kline_size  	 = quant.min_kline_size
		max_one_order_amount = quant.max_one_order_amount
		twap_interval        = quant.twap_interval

		# 获取U本位合约账户净值(不包含未实现盈亏)
		if debug:
			equity = 100000
		else:
			balance    = robust(exchange.fapiPrivate_get_balance, func_name='fapiPrivate_get_balance')  # 获取账户净值
			balance    = pd.DataFrame(balance)
			equity     = float(balance[balance['asset'] == 'USDT']['balance'])

		trade_usdt = equity*trade_ratio
		print('账户净值:', equity, '杠杆:', trade_ratio, '实际净值:', trade_usdt)


		# =====获取每个策略分配的资金：固定资金，之后的版本会改成浮动
		strategy_trade_usdt = cal_strategy_trade_usdt(stratagy_list, trade_usdt)
		# =====获取账户的实际持仓
		if debug:
			symbol_info = pd.DataFrame(index=symbol_list, columns=['当前持仓量'])
			symbol_info['当前持仓量'] = 0
		else:
			symbol_info = update_symbol_info(exchange, symbol_list)
		
		# =====sleep直到下一个整点小时
		if debug:
			run_time = datetime.strptime('2021-12-23 16:00:00', "%Y-%m-%d %H:%M:%S")
			#run_time = sleep_until_run_time('1h', if_sleep=False, cheat_seconds=0)
		else:
			run_time = sleep_until_run_time('1h', if_sleep=True,  cheat_seconds=0)

		now = datetime.now()
		# =====合约每日结算时api暂停1分钟
		if (now.hour % 8 == 0 and now.minute == 0):
			print('每日结算时间')
			time.sleep(60 - now.second)
		
		# =====并行获取所有币种的1小时K线
		s_time = time.time()
		symbol_candle_data = fetch_all_binance_swap_candle_data(exchange, symbol_list, run_time, njob1)
		print('获取所有币种K线数据完成，花费时间：', time.time() - s_time)

		# =====获取当前资金费率
		s_time = time.time()
		set_fundingrate(exchange)
		print('保存资金数据完成，花费时间：', time.time() - s_time)

		# =====选币数据整理 & 选币
		s_time = time.time()
		select_coin = cal_factor_and_select_coin(
			symbol_candle_data, stratagy_list, run_time, njob2, min_kline_size=min_kline_size
		)
		print(select_coin)
		print('完成选币数据整理 & 选币，花费时间：', time.time() - s_time)

		# =====开始计算&下单
		symbol_info = cal_order_amount(symbol_info, select_coin, strategy_trade_usdt)

		# 补全历史持仓的最新价格信息
		if symbol_info['实际下单资金'].isnull().any():
			symbol_last_price = fetch_binance_ticker_data(exchange)
			nan_symbol = symbol_info.loc[symbol_info['实际下单资金'].isnull(), '实际下单资金'].index
			symbol_info.loc[nan_symbol, '实际下单资金'] = symbol_info.loc[nan_symbol, '实际下单量'] * symbol_last_price[nan_symbol]

		# 使用twap算法拆分订单
		twap_symbol_info_list = get_twap_symbol_info_list(symbol_info, max_one_order_amount)

		for i in range(len(twap_symbol_info_list)):
			# =====获取币种的最新价格
			symbol_last_price = fetch_binance_ticker_data(exchange)
			# =====逐批下单
			if not debug:
				place_order(exchange, twap_symbol_info_list[i], symbol_last_price, min_qty, price_precision, min_notional)
			else:
				print('测试拆单', twap_symbol_info_list[i])
				continue
			# =====idle
			if i < len(twap_symbol_info_list) - 1 :
				print(f'Twap {twap_interval} s 等待')
				time.sleep(twap_interval)

		if not debug:
			replenish_bnb(exchange, balance)

		# 清理数据
		del symbol_candle_data, select_coin, symbol_info
		# 本次循环结束
		# 时间补偿
		print('-' * 20, '本次循环结束，%f秒后进入下一次循环' % 20, '-' * 20)
		print('\n')
		time.sleep(20)


if __name__=='__main__':
	while True:
		try:
			run()
		except Exception as err:
			print('系统出错，10s之后重新运行，出错原因: ' + str(err))
			print(traceback.format_exc())
			time.sleep(10)




