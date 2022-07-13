#!/usr/bin/python3
# -*- coding: utf-8 -*-

import os
import time
import pandas as pd
from datetime import datetime, timedelta


# 计算每个策略分配的资金
def cal_strategy_trade_usdt(stratagy_list, trade_usdt):
    df = pd.DataFrame()
    # 策略的个数
    strategy_num = len(stratagy_list)
    # 遍历策略
    for strategy in stratagy_list:
        c_factor    = strategy['c_factor']
        hold_period = strategy['hold_period']
        select_coin_num = strategy['select_coin_num']

        offset_num = int(hold_period[:-1])
        balance = trade_usdt/strategy_num/2/offset_num/select_coin_num
        for offset in range(offset_num):
            df.loc[f'{c_factor}_{hold_period}_{offset}H', '策略分配资金'] = balance

    df.reset_index(inplace=True)
    df.rename(columns={'index': 'key'}, inplace=True)

    return df






