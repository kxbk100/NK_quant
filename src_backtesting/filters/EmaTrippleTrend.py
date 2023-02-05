import numpy as np
import pandas_ta as ta  # 需要 pip install pandas_ta


# ema 三均线择时过滤短线升级版
# 默认均线参数 21 55 89
def signal(*args):
    df = args[0]
    params = args[1]
    factor_name = args[2]

    s, m, l = params

    df['ema_s'] = ta.ema(df['close'], length=s)
    df['ema_m'] = ta.ema(df['close'], length=m)
    df['ema_l'] = ta.ema(df['close'], length=l)

    short_risk_cond = df['high'].rolling(6).max() / df['low'].rolling(6).min() > 2
    long_trend_cond = (df['ema_s'] > df['ema_m']) & (df['ema_m'] > df['ema_l']) & (df['ema_s'] > df['ema_s'].shift()) & (df['ema_m'] > df['ema_m'].shift()) & (df['ema_l'] > df['ema_l'].shift())
    short_trend_cond = (df['ema_s'] < df['ema_m']) & (df['ema_m'] < df['ema_l']) & (df['ema_s'] < df['ema_s'].shift()) & (df['ema_m'] < df['ema_m'].shift()) & (df['ema_l'] < df['ema_l'].shift()) & (~short_risk_cond)
    df.loc[long_trend_cond, 'trend_ind'] = 1
    df.loc[short_trend_cond, 'trend_ind'] = -1
    df.loc[:, 'trend_ind'] = df['trend_ind'].fillna(0)
    df.loc[df['ema_l'].isnull(), 'trend_ind'] = np.nan

    df[factor_name] = df['trend_ind']

    return df