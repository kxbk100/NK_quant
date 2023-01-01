#!/usr/bin/python3
# -*- coding: utf-8 -*-

import os
import time
import numpy as np
import pandas as pd
from glob import glob
from datetime import datetime, timedelta
from joblib import Parallel, delayed
from utils  import diff

pd_display_rows  = 20
pd_display_cols  = 8
pd_display_width = 1000
pd.set_option('display.max_rows', pd_display_rows)
pd.set_option('display.min_rows', pd_display_rows)
pd.set_option('display.max_columns', pd_display_cols)
pd.set_option('display.width', pd_display_width)
pd.set_option('display.max_colwidth', pd_display_width)
pd.set_option('display.unicode.ambiguous_as_wide', True)
pd.set_option('display.unicode.east_asian_width', True)
pd.set_option('expand_frame_repr', False)
os.environ['NUMEXPR_MAX_THREADS'] = "256"

from warnings import simplefilter
simplefilter(action='ignore', category=FutureWarning)

from config import pickle_path, data_path, head_columns
from config import factor_class_list


def cal_one_factor(df, class_name, back_hour_list, diff_list):
    df = df.copy()
    """ ******************** 以下是需要修改的代码 ******************** """
    factor_list = []
    # =====技术指标
    _cls = __import__('factors.%s' % class_name,  fromlist=('', ))
    for n in back_hour_list:
        for d_num in diff_list:
            if d_num > 0:
                factor_name = f'{class_name}_bh_{n}_diff_{d_num}'
            else:
                factor_name = f'{class_name}_bh_{n}'
            factor_list.append(factor_name)  
            # 计算因子
            getattr(_cls, 'signal')(df, n, d_num, factor_name)
    """ ************************************************************ """
    factor_list.sort()
    df = df[head_columns + factor_list]
    df.sort_values(by=['candle_begin_time', ], inplace=True)
    df.reset_index(drop=True, inplace=True)

    return df, class_name


def run(trade_type, back_hour_list, diff_list, njobs=16):
    print('\n')
    print(f'trade_type --- {trade_type}')
    # ===创建因子存储目录
    all_factor_path = os.path.join(data_path, trade_type)
    if not os.path.exists(all_factor_path):
        os.makedirs(all_factor_path)

    # ===批量删除头文件
    # 按照下面的处理逻辑, 如果头文件存在就不做覆盖操作
    # 如果重新计算该脚本, 默认头文件不会替换, 可能造成数据错乱
    for header_file in glob(os.path.join(all_factor_path, '*', 'coin_alpha_head.pkl')):
        if os.path.exists(header_file):
            os.remove(header_file)

    # ===开始计算因子
    for pkl_file in glob(os.path.join(pickle_path, f'{trade_type}', '*USDT.pkl')):
        print('    ', pkl_file)
        symbol = os.path.basename(pkl_file).replace('.pkl', '')
        df = pd.read_feather(pkl_file)

        # =====处理原始数据
        df.sort_values(by='candle_begin_time', inplace=True)
        df.drop_duplicates(subset=['candle_begin_time'], inplace=True, keep='last') 
        df['下个周期_avg_price'] = df['avg_price'].shift(-1)  # 计算下根K线开盘买入涨跌幅
        df.reset_index(drop=True, inplace=True)

        '''串行
        results = []
        for cls_name in factor_class_list:
            result = cal_one_factor(df, cls_name, back_hour_list, diff_list)
            results.append(result)
        '''

        # '''并行
        results = Parallel(n_jobs=njobs)(
            delayed(cal_one_factor)(
                df, cls_name, back_hour_list, diff_list
            )
            for cls_name in factor_class_list
        )
        for df, class_name in results:
            symbol_factor_path = os.path.join(all_factor_path, symbol)
            if not os.path.exists(symbol_factor_path):
                os.makedirs(symbol_factor_path)
            # 保存文件头
            head_path = os.path.join(symbol_factor_path, f'coin_alpha_head.pkl')
            if not os.path.exists(head_path):
                df_head = df[head_columns]
                df_head.to_feather(head_path) 
            # 保存因子
            factor_path = os.path.join(symbol_factor_path, f'coin_alpha_factor_{class_name}.pkl')
            df_factors  = df[list(set(df.columns) - set(head_columns))]
            df_factors.to_feather(factor_path) 


if __name__ == '__main__':
    back_hour_list  = [3, 4, 6, 8, 9, 12, 24, 30, 36, 48, 60, 72, 96]  
    # ===计算因子
    for trade_type in ['spot', 'swap'][1:]:
        run(trade_type, back_hour_list, diff_list=[0])












