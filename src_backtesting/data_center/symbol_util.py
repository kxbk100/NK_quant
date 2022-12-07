#!/usr/bin/python3
# -*- coding: utf-8 -*-
"""
本代码由joey提供，用于获取币安历史数据
20210730

## 说明
1. 币安数据中心内容很多，取需要的有针对性地进行下载
本次准备获取 data/spot/monthly/klines/(币种)/1m/数据
2. 直接采用币安存放数据的目录结构存放数据
3. 币安是动态网址，'https://s3-ap-northeast-1.amazonaws.com/data.binance.vision?delimiter=/&prefix='
4. 如果数据超过1000条，有截断istruncated,需要根据首页的marker 生成新的网址来获得下页，代码中已实现
6. 币安历史上所有现货币种，存在'../data/currencies_lst.csv'中
5. 1m数据文件的链接地址获取完成后，存在“data/all_files_lst.csv”中
6. 如果有报错，存在“data/_ log.csv”文件中。

## 使用方法
1. 两处参数用于修改
    修改设定参数
    # 设定参数1
    interval = '1m'                 # K线时间
    month_time = '2021-06'          # 数据更新至什么时间
    at_mobiles = ['xxxxxxx']    # 钉钉@谁
    prefix_lst = ['data/spot/monthly/klines/',      # 用于获取币种的prefix
                  ]

    # 设定参数2
    prefix_lst = [prefix_lst[0] + i + '/{}/'.format(interval) for i in currencies_lst]  # 通过currencies_lst创建
2.如果是合约，改为合约数据对应的prefix即可

## 具体做法
###
创建get_link_lst(url, nextmarker=None)，用于获取单个prefix的下级目录和文件
创建get_next_links_lst(prefix_lst, next_links_lst=[])，用于获取获取多个prefix（即prefix_lst）的下级目录和文件
创建get_all_files_lst(prefix_lst, all_files=[])，用于获取多个prefix（即prefix_lst）的下级\下下级\...目录和文件,直到获取全部文件
"""
import time
from xml.dom import minidom

import requests

root_center_url = 'https://s3-ap-northeast-1.amazonaws.com/data.binance.vision'
# 获取r.text,用于网页中包含的prefix目录
def get_url_rText(params, proxy='', try_nums=50, data=None):
    if proxy == '' or proxy is None:
        input_proxy = None
    else:
        input_proxy = {'https': proxy}
    for i in range(try_nums):
        try:
            r = requests.get(url=root_center_url, params=params, proxies=input_proxy, timeout=60)
            data = r.text
            break
        except Exception as e:
            print('Exception', e, type(e))
            if i == 0:
                print(root_center_url, "连接被拒，5s后重连:", end=' ')
            print(i, end=' ')
            time.sleep(5)
            continue
    return data


# 获取单个prefix的下级目录和文件
def get_link_lst(params, proxies=''):
    data = get_url_rText(params, proxies, try_nums=50)  # 使用增加了容错的函数
    # print(data)
    domTree = minidom.parseString(data)
    rootNode = domTree.documentElement
    items = rootNode.getElementsByTagName('CommonPrefixes')
    IsTruncated = rootNode.getElementsByTagName('IsTruncated')[0]
    # print(IsTruncated.childNodes[0].data)
    link_lst = []
    for item in items:
        pr = item.getElementsByTagName('Prefix')[0]
        # print(pr.childNodes[0].data)
        param = pr.childNodes[0].data
        s = param.split('/')
        link_lst.append(s[len(s) - 2])
    if IsTruncated.childNodes[0].data == 'true':
        nextmarker = rootNode.getElementsByTagName('NextMarker')[0]
        nextmarker = nextmarker.childNodes[0].data
        # 下一页的网址
        params['marker'] = nextmarker
        link_lst.extend(get_link_lst( params, proxies))  # 初次循环时，link_lst 包含1000条以上的数据
    return link_lst


# 获取多个prefix（即prefix_lst）的下级目录和文件
def get_all_symbols(prefix, proxy=''):
    params = {
        'delimiter': '/',
        'prefix': prefix
    }
    next_links_lst = []
    # print('prefix:', prefix)
    link_lst = get_link_lst(params, proxy)
    [next_links_lst.append(i) for i in link_lst if i.endswith('USDT') and not i in next_links_lst]  # 去重添加
    print('所有交易对数量为：', len(next_links_lst))
    print(next_links_lst)
    return next_links_lst

def get_item_list(params, proxies):
    data = get_url_rText(params, proxies, try_nums=50)
    domTree = minidom.parseString(data)
    rootNode = domTree.documentElement
    items = rootNode.getElementsByTagName('Contents')
    IsTruncated = rootNode.getElementsByTagName('IsTruncated')[0]
    result = []
    for item in items:
        key = item.getElementsByTagName('Key')[0]
        # print(pr.childNodes[0].data)
        param = key.childNodes[0].data
        if str(param).endswith('CHECKSUM'):
            last_modified = item.getElementsByTagName('LastModified')[0]
            struct_time = time.strptime(last_modified.childNodes[0].data, '%Y-%m-%dT%H:%M:%S.%fZ')
            _tmp = {
                'key': param,
                'last_modified': time.mktime(struct_time)
            }
            result.append(_tmp)
    if IsTruncated.childNodes[0].data == 'true':
        nextmarker = rootNode.getElementsByTagName('NextMarker')[0]
        nextmarker = nextmarker.childNodes[0].data
        # 下一页的网址
        params['marker'] = nextmarker
        result.extend(get_item_list(params, proxies))
    return result


def get_symbol_items(prefix, marker, proxy=''):
    param = {
        'delimiter': '/',
        'prefix': prefix,
        'marker': marker
    }
    return get_item_list(param, proxy)

