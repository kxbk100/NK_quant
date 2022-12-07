#!/usr/bin/python3
# -*- coding: utf-8 -*-
import datetime
import os.path
from concurrent.futures import ThreadPoolExecutor
from glob import glob
from hashlib import sha256
from socket import timeout
from xml.dom import minidom

import pandas as pd
import requests
from data_center import symbol_util
from dateutil.relativedelta import relativedelta
from joblib import Parallel, delayed

BASE_URL = 'https://data.binance.vision/'


# def get_usdt_symbols(_type="swap"):
#     '''
#     ' 获取当前能正常现货/合约交易的usdt symbol
#     '''
#     # 获取U本位现货/合约交易对
#     # 返回所有的USDT交易对名称
#     exchange = ccxt.binance()
#     exchange.proxies = {
#         'http': 'http://127.0.0.1:8889',
#         'https': 'http://127.0.0.1:8889'
#     }
#     market = None
#     if _type == 'spot':
#         market = exchange.publicGetExchangeInfo()
#     elif _type == 'swap':
#         market = exchange.fapiPublicGetExchangeInfo()
#
#     symbols = []
#     # symbols = {}
#     for symbol in market['symbols']:
#         # symbols[symbol['symbol']] = datetime.utcfromtimestamp(int(symbol['onboardDate']) / 1000)
#         symbols.append(symbol['symbol'])
#     # print(symbols)
#     return symbols


def getSha256(strFilePath):
    if strFilePath:
        sha256Obj = sha256()
        with open(strFilePath, 'rb') as f:
            sha256Obj.update(f.read())
        return sha256Obj.hexdigest().lower()


def getChecksum(strFilePath):
    if strFilePath:
        with open(strFilePath, encoding='utf-8') as f:
            data = f.readline()
            return data.split(' ')[0]


def download_all(symbol, trading_type, data_type, intervals, folder, _njobs, https_proxy):
    session = requests.Session()
    print(intervals)
    intervals_pool = ThreadPoolExecutor(2)
    for interval in intervals:
        intervals_pool.submit(download_single_interval, session, symbol, trading_type, data_type, interval, folder, _njobs, https_proxy)
    intervals_pool.shutdown(True)
    session.close()


def download_single_interval(session, symbol, trading_type, data_type, interval, folder, _njobs, https_proxy):
    # 创建下载线程池，最多维护_njobs个线程
    threadpool = ThreadPoolExecutor(_njobs)
    today = datetime.date.today()
    this_month_first_day = datetime.date(today.year, today.month, 1)
    daily_end = this_month_first_day - relativedelta(months=1)

    error_monthly_uri = []
    error_daily_uri = []
    monthly_zip_list = []
    daily_zip_list = []
    daily_path = get_local_path(folder, trading_type, data_type, "daily", symbol, interval)
    daily_prefix = get_download_prefix(trading_type, data_type, 'daily', symbol, interval)
    checksum_file_name = "{}-{}-{}.zip.CHECKSUM".format(symbol.upper(), interval, daily_end - relativedelta(days=1))
    first_checksum_file_uri = '{}{}'.format(daily_prefix, checksum_file_name)
    # 爬取页面的daily list
    checksum_uri_list = symbol_util.get_symbol_items(daily_prefix, first_checksum_file_uri, https_proxy)
    # 1.下载本月和上月的daily数据
    for checksum_uri in checksum_uri_list:
        threadpool.submit(download_symbol_data_item, 'daily', session, daily_path, checksum_uri, daily_zip_list, error_daily_uri)

    monthly_path = get_local_path(folder, trading_type, data_type, "monthly", symbol, interval)
    tmp_date = daily_end - relativedelta(months=1)
    this_month = str(tmp_date)[0:-3]
    monthly_prefix = get_download_prefix(trading_type, data_type, 'monthly', symbol, interval)
    checksum_file_name = "{}-{}-{}.zip.CHECKSUM".format(symbol.upper(), interval, this_month)
    first_checksum_file_uri = '{}{}'.format(monthly_prefix, checksum_file_name)
    # 爬取页面的monthly list
    checksum_uri_list = symbol_util.get_symbol_items(monthly_prefix, '', https_proxy)
    # 下载上月之前的monthly数据
    for checksum_uri in checksum_uri_list:
        if checksum_uri['key'] <= first_checksum_file_uri:
            threadpool.submit(download_symbol_data_item, 'monthly', session, monthly_path, checksum_uri, monthly_zip_list, error_monthly_uri)

    threadpool.shutdown(True)  # 等待线程池中的任务执行完毕后，继续执行

    if len(error_daily_uri) > 0 or len(error_monthly_uri) > 0:
        # 开始处理下载失败的数据包
        retrypool = ThreadPoolExecutor(2)
        if len(error_daily_uri) > 0:
            retrypool.submit(retry_download, 'daily', session, daily_path, daily_zip_list, error_daily_uri)
        if len(error_monthly_uri) > 0:
            retrypool.submit(retry_download, 'monthly', session, daily_path, monthly_zip_list, error_monthly_uri)
        retrypool.shutdown(True)

    # 开始清理之前的daily数据,使得daily文件夹下始终只有本月和上月的daily数据
    oldest_zip_path = os.path.join(daily_path, "{}-{}-{}.zip".format(symbol.upper(), interval, daily_end))
    daily_list = glob(os.path.join(daily_path, '*'))
    if len(daily_list) > 0:
        for daily_file_path in daily_list:
            if daily_file_path < oldest_zip_path:
                # print('remove {}'.format(daily_file_path))
                os.remove(daily_file_path)

    zip_file_prefix = f'{symbol.upper()}-{interval}-'
    # 将daily数据合并成monthly数据
    transfer_daily_to_monthly(daily_zip_list, today, this_month_first_day, daily_end, monthly_path, zip_file_prefix)


def retry_download(_type, session, daily_path, daily_zip_list, error_uri_list):
    while len(error_uri_list) > 0:
        print(f'retry download {_type} error_uri length = {len(error_uri_list)}')
        for checksum_uri in error_uri_list:
            download_symbol_data_item(_type, session, daily_path, checksum_uri, daily_zip_list, error_uri_list)
        # print('remain error_uri', error_uri_list)


def download_symbol_data_item(_type, session, local_path, checksum_uri, correct_zip_list, error_uri):
    """
    下载某天或某月的checksum文件和zip文件
    """
    checksum_file_name = checksum_uri['key'].split('/')[-1]
    download_checksum_url = f'{BASE_URL}{checksum_uri["key"]}'
    checksum_file_path = os.path.join(local_path, checksum_file_name)
    if os.path.exists(checksum_file_path):
        '''
        这里对checksum文件的更新时间与币安数据中心的更新时间作比较
        '''
        modify_utc_timestamp = datetime.datetime.utcfromtimestamp(os.path.getmtime(checksum_file_path)).timestamp()
        if modify_utc_timestamp < checksum_uri['last_modified']:
            os.remove(checksum_file_path)

    zip_file_name = checksum_file_name[0: -9]
    download_zip_url = download_checksum_url[0: -9]
    # 下载一个文件有2次重试机会
    retry = 2
    while retry > 0:
        retry -= 1
        try:
            # 下载checksum_file
            sumPath = download_file(session, local_path, checksum_file_name, download_checksum_url)
            if sumPath:
                # 下载zip_file
                zipPath = download_file(session, local_path, zip_file_name, download_zip_url)
                if zipPath:
                    sha256Str = getSha256(zipPath)
                    remoteSha = getChecksum(checksum_file_path)
                    # print('localsha256:', sha256Str)
                    # print('remoteSha:', remoteSha)
                    if sha256Str == remoteSha:
                        # print('{} correct'.format(zip_file_name))
                        correct_zip_list.append(zipPath)
                        if checksum_uri['key'] in error_uri:
                            error_uri.remove(checksum_uri['key'])
                        return
                    else:
                        # CHECKSUM文件与zip文件不匹配，删除CHECKSUM文件和zip
                        # print('delete error zip and CHECKSUM file')
                        os.remove(zipPath)
                        os.remove(sumPath)
        except timeout:
            # 请求超时重试
            print('timeout')
            retry += 1
            continue
        except Exception:
            continue
    # 重试3次仍然失败，加入error_zip列表
    if checksum_uri['key'] not in error_uri:
        error_uri.append(checksum_uri['key'])
    print(f'出现下载失败的zip_file:{zip_file_name},已加入错误列表,稍后重试')


def download_file(session, local_path, file_name, download_url):
    save_path = os.path.join(local_path, file_name)

    if os.path.exists(save_path):
        # print("file already exists! {}".format(save_path))
        return save_path

    # make the directory
    if not os.path.exists(local_path):
        os.makedirs(local_path)

    dl_file = session.get(download_url, timeout=5)
    if dl_file.status_code == 404:
        domTree = minidom.parseString(dl_file.content)
        rootNode = domTree.documentElement
        code = rootNode.getElementsByTagName('Code')[0]
        if code.childNodes[0].data == 'NoSuchKey':
            return
    if dl_file.status_code == 200:
        with open(save_path, 'wb') as out_file:
            out_file.write(dl_file.content)
        return save_path
    print('unknown status ', dl_file.status_code)
    print(dl_file.content)


def transfer_daily_to_monthly(daily_zip_list, today, this_month_first_day, daily_end, monthly_path, zip_file_prefix):
    # 将当月和上月daily数据转成monthly数据
    this_month = []
    last_month = []
    for zip_file in daily_zip_list:
        if str(today) > zip_file[-14: -4] >= str(this_month_first_day):
            this_month.append(zip_file)
        if str(this_month_first_day) > zip_file[-14: -4] >= str(daily_end):
            last_month.append(zip_file)
    if len(last_month) > 0 or len(this_month) > 0:
        if not os.path.exists(monthly_path):
            os.makedirs(monthly_path)
    if len(last_month) > 0:
        df_last_month = pd.concat(Parallel(4)(
            delayed(pd.read_csv)(path_, header=None, encoding="utf-8", compression='zip') for path_ in last_month),
                ignore_index=True)
        df_last_month = df_last_month[df_last_month[0] != 'open_time']
        df_last_month.to_csv(os.path.join(monthly_path, f'{zip_file_prefix}{str(daily_end)[0: -3]}.zip'), header=None,
                             index=None, compression='zip')
    if len(this_month) > 0:
        df_this_month = pd.concat(Parallel(4)(
            delayed(pd.read_csv)(path_, header=None, encoding="utf-8", compression='zip') for path_ in this_month),
                ignore_index=True)
        df_this_month = df_this_month[df_this_month[0] != 'open_time']
        df_this_month.to_csv(os.path.join(monthly_path, f'{zip_file_prefix}{str(today)[0: -3]}.zip'), header=None,
                             index=None, compression='zip')


def get_local_path(root_path, trading_type, market_data_type, time_period, symbol, interval='5m'):
    trade_type_folder = trading_type if '5m' == interval else trading_type + '_1m'
    path = os.path.join(root_path, trade_type_folder, f'{time_period}_{market_data_type}')

    if symbol:
        path = os.path.join(path, symbol.upper())
    return path


def get_download_prefix(trading_type, market_data_type, time_period, symbol, interval):
    trading_type_path = 'data/spot'
    if trading_type == 'swap':
        trading_type_path = 'data/futures/um'
    return f'{trading_type_path}/{time_period}/{market_data_type}/{symbol.upper()}/{interval}/'


def write_to_bz2(bz2_path, bz2_file_path, zip_list, _njobs):
    # 合并monthly daily 数据
    df = pd.concat(
        Parallel(_njobs)(delayed(pd.read_csv)(path_, header=None, encoding="utf-8", compression='zip',
                                              names=['open_time', 'open', 'high', 'low', 'close', 'volume',
                                                     'close_time', 'quote_volume', 'trade_num',
                                                     'taker_buy_base_asset_volume', 'taker_buy_quote_asset_volume',
                                                     'ignore']
                                              ) for path_ in zip_list), ignore_index=True)
    df = df[df['open_time'] != 'open_time']  # 解决新zip包含header问题
    df['candle_begin_time'] = pd.to_datetime(df['open_time'], unit='ms')
    # 删除无用或暂时用不上的列
    del df['open_time']
    del df['close_time']
    del df['ignore']
    # df.reset_index(drop=True).to_feather(pkl_file_path)
    df.to_csv(bz2_file_path, index=False, encoding='utf-8', mode='w', compression='bz2')

    # print(pd.read_csv(pkl_file_path, encoding='utf-8', compression='bz2', parse_dates=['candle_begin_time']))
