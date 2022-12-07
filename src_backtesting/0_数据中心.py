#!/usr/bin/python3
# -*- coding: utf-8 -*-


import datetime
from multiprocessing import cpu_count

from config import root_path
from data_center import download_util, symbol_util


def download_klines(trading_type, symbols, data_type, intervals, folder, https_proxy=''):
    current = 0
    print("Found {} symbols".format(len(symbols)))

    # 并行数
    njobs = cpu_count() - 2
    start_time = datetime.datetime.now()
    for symbol in symbols:
        startTime = datetime.datetime.now()
        print("[{}/{}] - start download {} {} ".format(current + 1, len(symbols), symbol, data_type))
        download_util.download_all(symbol, trading_type, data_type, intervals, folder, njobs, https_proxy)
        endTime = datetime.datetime.now()
        print("[{}/{}] - end download {} {} cost {} s".format(current + 1, len(symbols), symbol, data_type,
                                                                  (endTime - startTime).seconds))
        current += 1

    end_time = datetime.datetime.now()
    print("download end cost {} s".format((end_time - start_time).seconds))


def run():
    # swap数据类别
    SWAP_DATA_TYPE = ['klines', 'indexPriceKlines', 'markPriceKlines', 'premiumIndexKlines']
    # spot数据类别
    SPOT_DATA_TYPE = ['klines']

    # 合约prefix参数
    swap_prefix = 'data/futures/um/daily/'
    # 现货prefix参数
    spot_prefix = 'data/spot/daily/'

    # todo 按需修改数据类别data_type
    data_type = SWAP_DATA_TYPE[0]
    # todo 按需修改prefix,下载合约数据用swap_prefix,下载现货数据用spot_prefix
    prefix = swap_prefix + data_type + '/'
    # todo 按照个人情况更改代理地址https_proxy,若不需要代理,该参数设为空''或者None
    # https_proxy = 'http://127.0.0.1:8889'
    https_proxy = ''
    # 获取所有USDT交易对
    symbols = symbol_util.get_all_symbols(prefix, https_proxy)

    # symbols = ['BTCUSDT', 'ETHUSDT']
    # 下载目录
    # folder = '/media/market/BinanceData'
    # todo 下载目录可指定其他目录，这里默认在中性框架当前目录下
    sub_path = '/data/binance-center'
    folder = root_path+sub_path
    # folder = '/media/moke/新加卷/market/data'
    # folder = 'D:\market'

    # spot 现货 swap U本位合约
    TRADING_TYPE = ["spot", "swap"]

    # 下载swap_symbols完整数据
    download_klines(TRADING_TYPE[1], symbols, data_type, ['1m', '5m'], folder, https_proxy)

    # 下载spot_symbols完整数据
    # todo 如果要下载现货K线数据，请打开注释
    # download_klines(TRADING_TYPE[0], symbols, data_type, ['1m', '5m'], folder, https_proxy)




if __name__ == "__main__":
    run()
