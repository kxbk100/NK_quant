#!/usr/bin/python3
# -*- coding: utf-8 -*-

import os
import time
import pandas as pd
from datetime import datetime, timedelta

from utils.commons import robust


def transfer_future_to_spot(exchange, asset, amount):
    params = {
        'type':   2,      # 1：现货至u本位合约；2：u本位合约至现货
        'asset':  asset,
        'amount': amount,
    }
    return robust(exchange.sapiPostFuturesTransfer, params=params, func_name='sapiPostFuturesTransfer')


def transfer_spot_to_future(exchange, asset, amount):
    params = {
        'type':   1,      # 1：现货至u本位合约；2：u本位合约至现货
        'asset':  asset,
        'amount': amount,
    }
    return robust(exchange.sapiPostFuturesTransfer, params=params, func_name='sapiPostFuturesTransfer')


def spot_buy_quote(exchange, symbol, quote_amount):
    params = {
        'symbol':        symbol,
        'side':          'BUY',
        'type':          'MARKET',
        'quoteOrderQty': quote_amount
    }
    return robust(exchange.privatePostOrder, params=params, func_name='privatePostOrder')


def get_spot_balance(exchange, asset):
    account = robust(exchange.private_get_account, func_name='private_get_account')
    balance = account['balances']
    balance = pd.DataFrame(balance)
    # 如果子账号没有使用过现货账户，此处会返回空值
    if balance.empty:
        return 0.0

    amount = float(balance[balance['asset'] == asset]['free'])
    print(f'查询到现货账户有{amount} {asset}')
    return amount


def replenish_bnb(exchange, balance, amount_t=10):
    if amount_t == 0:
        return

    min_bnb = 0.001     # 该参数在BNB达到 10000 USDT之前有效
    amount_bnb = float(balance[balance['asset'] == 'BNB']['balance'].iloc[0])
    print(f"当前账户剩余{amount_bnb} BNB")
    
    if amount_bnb < min_bnb:
        spot_bnb_amount = get_spot_balance(exchange, 'BNB')
        print(f"当前现货账户持有{spot_bnb_amount} BNB")

        if spot_bnb_amount < min_bnb:
            print("从现货市场买入10 USDT等值BNB并转入合约账户")

            spot_usdt_amount = get_spot_balance(exchange, 'USDT')
            if spot_usdt_amount < amount_t:
                transfer_future_to_spot(exchange, 'USDT', amount_t - spot_usdt_amount)
            spot_buy_quote(exchange, 'BNBUSDT', amount_t)
            
            time.sleep(2)

            retry = 0
            # 如果获取到现货账户BNB持仓扔小于最小BNB量，说明币安账户未更新，在行情剧烈波动的情况下存在这种情况
            # 睡眠20秒后重新获取BNB现货账户余额，重试15次（5分钟）后仍未更新则放弃
            while spot_bnb_amount < min_bnb and retry < 3:
                spot_bnb_amount = get_spot_balance(exchange, 'BNB')
                if spot_bnb_amount > min_bnb:
                    break
                else:
                    retry += 1
                    time.sleep(3)

        transfer_spot_to_future(exchange, 'BNB', spot_bnb_amount)
        print(f"成功买入{spot_bnb_amount} BNB并转入U本位合约账户")













