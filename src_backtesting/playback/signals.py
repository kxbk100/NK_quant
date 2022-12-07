# # -*-coding:utf-8-*-
# import sys
# import io
# sys.stdout = io.TextIOWrapper(sys.stdout.buffer,encoding='gb18030')
import numpy as np
import pandas as pd
def ma_signal(param, arr):
    # 通过返回trade_ratio达到择时效果，trade_ratio最小为 0.1
    min_length = param[-1]
    n = param[0]
    if len(arr) < min_length:
        return 1
    else:
        if arr[-1] > np.nanmean(arr[-n:]):
            trade_ratio = 1
        else:
            trade_ratio = 0.2
    return trade_ratio
def curve_timming_adapt_signal(params,arr):

    min_length = params[-1]
    if len(arr) < min_length:
        return 1
    mix_curve_ = pd.DataFrame(arr,columns=['raw_curve'])
    up_t=0
    down_t=0
    up_p = 0.5
    down_p = 0.1
    pct_period = params[0]
    buy_trade_ratio = 1
    sell_trade_ratio = 0.1
    up_t = up_t
    down_t = down_t
    mix_curve_['change'] = mix_curve_['raw_curve'].pct_change().fillna(0)
    mix_curve_['min_change'] = mix_curve_['raw_curve'].pct_change(int(pct_period/10))
    mix_curve_['period_change'] = mix_curve_['raw_curve'].pct_change(pct_period).fillna(1)
    mix_curve_['up_change'] = mix_curve_['min_change'].rolling(window=pct_period).quantile(up_p).fillna(up_t)
    mix_curve_.loc[mix_curve_['up_change']<=up_t,'up_change'] = up_t
    mix_curve_['down_change'] = mix_curve_['min_change'].rolling(window=pct_period).quantile(down_p).fillna(-down_t)
    mix_curve_.loc[mix_curve_['down_change']>=-down_t,'down_change'] = -down_t
    mix_curve_.loc[mix_curve_['period_change']>=mix_curve_['up_change'],'_signal'] = buy_trade_ratio
    mix_curve_.loc[mix_curve_['period_change']<mix_curve_['down_change'],'_signal'] = sell_trade_ratio
    mix_curve_['_signal'] = mix_curve_['_signal'].fillna(method='ffill')

    mix_curve_.loc[mix_curve_['_signal'] != mix_curve_['_signal'].shift(1),'dk'] = 1
    mix_curve_['dk'] = mix_curve_['dk'].fillna(0)
    mix_curve_['14dk'] = mix_curve_['dk'].rolling(14*24).sum()

    mix_curve_.loc[mix_curve_['14dk']>=4,'up_change'] = 2 * mix_curve_.loc[mix_curve_['14dk']>=4,'up_change']
    mix_curve_.loc[mix_curve_['14dk']>=4,'down_change'] = 2 * mix_curve_.loc[mix_curve_['14dk']>=4,'down_change']

    mix_curve_.loc[mix_curve_['period_change']>=mix_curve_['up_change'],'signal'] = buy_trade_ratio
    mix_curve_.loc[mix_curve_['period_change']<mix_curve_['down_change'],'signal'] = sell_trade_ratio
    mix_curve_['signal'] = mix_curve_['signal'].fillna(method='ffill')
    df = mix_curve_.iloc[:][['signal']]
    return df.iloc[-1,0]