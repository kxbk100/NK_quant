#!/usr/bin/python3
# -*- coding: utf-8 -*-

import os
import time
import pandas as pd
from datetime import datetime, timedelta
from multiprocessing import Pool
import traceback

from utils.commons import robust, robust_


# =====获取数据
# 获取单个币种的1小时数据
def fetch_binance_swap_candle_data(exchange, symbol, run_time, limit=1500):
    try:
        start_time_dt = run_time - timedelta(hours=limit)
        params = {
            'symbol':    symbol, 
            'interval':  '1h', 
            'limit':     limit,
            'startTime': int(time.mktime(start_time_dt.timetuple())) * 1000
        }
        # ===call KLine API
        kline = robust_(exchange.fapiPublic_get_klines, params=params, func_name='fapiPublic_get_klines')
        # 将数据转换为DataFrame
        columns = [
            'candle_begin_time', 
            'open', 
            'high', 
            'low', 
            'close', 
            'volume', 
            'close_time', 
            'quote_volume', 
            'trade_num',
            'taker_buy_base_asset_volume', 
            'taker_buy_quote_asset_volume', 
            'ignore'
        ]
        df = pd.DataFrame(kline, columns=columns, dtype='float')

        # 兼容时区
        utc_offset = int(time.localtime().tm_gmtoff/60/60)
        # 整理数据
        df['candle_begin_time'] = pd.to_datetime(df['candle_begin_time'], unit='ms') + pd.Timedelta(hours=utc_offset)  # 时间转化为东八区
        df['symbol'] = symbol  # 添加symbol列
        columns = [
            'symbol', 
            'candle_begin_time', 
            'open', 
            'high', 
            'low', 
            'close', 
            'volume', 
            'quote_volume',
            'trade_num',
            'taker_buy_base_asset_volume', 
            'taker_buy_quote_asset_volume',
        ]
        df = df[columns]

        df.sort_values(by=['candle_begin_time'], inplace=True)
        df.drop_duplicates(subset=['candle_begin_time'], keep='last', inplace=True)
        # 删除runtime那行的数据，如果有的话
        df = df[df['candle_begin_time'] < run_time]
        df.reset_index(drop=True, inplace=True)
        
        return symbol, df
    except Exception as e:
        print(traceback.format_exc())
        return symbol, None
        

# 并行获取所有币种永续合约数据的1小时K线数据
def fetch_all_binance_swap_candle_data(exchange, symbol_list, run_time, njob1):
    # 创建参数列表
    arg_list = [(exchange, symbol, run_time) for symbol in symbol_list]

    if njob1 == 1:    
        # 调试模式下，循环获取数据
        result = []
        for arg in arg_list:
            (exchange, symbol, run_time) = arg
            res = fetch_binance_swap_candle_data(exchange, symbol, run_time)
            result.append(res)
    else:
        # 多进程获取数据
        with Pool(processes=njob1) as pl:
            # 利用starmap启用多进程信息
            result = pl.starmap(fetch_binance_swap_candle_data, arg_list)
  
    return dict(result)


# 获取当前资金费率
def fetch_fundingrate(exchange):
    data = robust(exchange.fapiPublic_get_premiumindex, func_name='fapiPublic_get_premiumindex')
    data = [[row['time'], row['symbol'], row['lastFundingRate']] for row in data]
    df = pd.DataFrame(data, columns=['candle_begin_time', 'symbol', 'fundingRate'], dtype='float')
    # 处理日期格式
    df['candle_begin_time'] = (df['candle_begin_time']//1000) * 1000
    df['candle_begin_time'] = pd.to_datetime(df['candle_begin_time'], unit='ms')
    df['candle_begin_time'] = df['candle_begin_time'].apply(lambda x: pd.to_datetime(x.to_pydatetime().replace(minute=0, second=0, microsecond=0).strftime("%Y-%m-%d %H:%M:%S")))
    utc_offset = int(time.localtime().tm_gmtoff/60/60)
    df['candle_begin_time'] = df['candle_begin_time'] + pd.Timedelta(hours=utc_offset) 
    df.sort_values(by=['candle_begin_time', 'symbol'], inplace=True)
    df.reset_index(drop=True, inplace=True)

    return df


# 获取币安的ticker数据
def fetch_binance_ticker_data(exchange):
    tickers = robust(exchange.fapiPublic_get_ticker_24hr, func_name='fapiPublic_get_ticker_24hr')
    tickers = pd.DataFrame(tickers, dtype=float)
    tickers.set_index('symbol', inplace=True)

    return tickers['lastPrice']



