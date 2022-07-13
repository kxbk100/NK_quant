#!/usr/bin/python3
# -*- coding: utf-8 -*-

import os
import itertools
import numpy  as np
import pandas as pd
pd.set_option('expand_frame_repr', False)
pd.set_option('display.max_rows',  150)  # 最多显示数据的行数


def cal_ind(_select_c):
    if _select_c.empty:
        return None
        
    select_coin = _select_c.copy()

    # =====计算统计指标
    results = pd.DataFrame()
    results.loc[0, '累积净值'] = round(select_coin['资金曲线'].iloc[-1], 2)

    # ===计算最大回撤，最大回撤的含义：《如何通过3行代码计算最大回撤》https://mp.weixin.qq.com/s/Dwt4lkKR_PEnWRprLlvPVw
    # 计算当日之前的资金曲线的最高点
    select_coin['max2here'] = select_coin['资金曲线'].expanding().max()
    # 计算到历史最高值到当日的跌幅，drowdwon
    select_coin['dd2here'] = select_coin['资金曲线'] / select_coin['max2here'] - 1
    # 计算最大回撤，以及最大回撤结束时间
    end_date, max_draw_down = tuple(select_coin.sort_values(by=['dd2here']).iloc[0][['candle_begin_time', 'dd2here']])
    # 计算最大回撤开始时间
    start_date = select_coin[select_coin['candle_begin_time'] <= end_date].sort_values(by='资金曲线', ascending=False).iloc[0]['candle_begin_time']
    # 将无关的变量删除
    #select_coin.drop(['max2here', 'dd2here'], axis=1, inplace=True)

    results.loc[0, '最大回撤'] = format(max_draw_down, '.2%')
    results.loc[0, '最大回撤开始时间'] = str(start_date)
    results.loc[0, '最大回撤结束时间'] = str(end_date)

    # ===统计每个周期
    results.loc[0, '盈利周期数'] = len(select_coin.loc[select_coin['本周期多空涨跌幅'] > 0])  # 盈利笔数
    results.loc[0, '亏损周期数'] = len(select_coin.loc[select_coin['本周期多空涨跌幅'] <= 0])  # 亏损笔数
    results.loc[0, '胜率'] = format(results.loc[0, '盈利周期数'] / len(select_coin), '.2%')  # 胜率
    results.loc[0, '每周期平均收益'] = format(select_coin['本周期多空涨跌幅'].mean(), '.2%')  # 每笔交易平均盈亏
    results.loc[0, '盈亏收益比'] = round(select_coin.loc[select_coin['本周期多空涨跌幅'] > 0]['本周期多空涨跌幅'].mean() / \
        select_coin.loc[select_coin['本周期多空涨跌幅'] <= 0]['本周期多空涨跌幅'].mean() * (-1), 2)  # 盈亏比
    results.loc[0, '单周期最大盈利'] = format(select_coin['本周期多空涨跌幅'].max(), '.2%')  # 单笔最大盈利
    results.loc[0, '单周期大亏损'] = format(select_coin['本周期多空涨跌幅'].min(), '.2%')  # 单笔最大亏损

    # ===连续盈利亏损
    results.loc[0, '最大连续盈利周期数'] = max([len(list(v)) for k, v in itertools.groupby(np.where(select_coin['本周期多空涨跌幅'] > 0, 1, np.nan))])  # 最大连续盈利次数
    results.loc[0, '最大连续亏损周期数'] = max([len(list(v)) for k, v in itertools.groupby(np.where(select_coin['本周期多空涨跌幅'] <= 0, 1, np.nan))])  # 最大连续亏损次数

    # ===计算年化收益
    #annual_return = (select_coin['资金曲线'].iloc[-1] / select_coin['资金曲线'].iloc[0]) ** (
    #    '1 days 00:00:00' / (select_coin['candle_begin_time'].iloc[-1] - select_coin['candle_begin_time'].iloc[0]) * 365) - 1
    time_during   = select_coin.iloc[-1]['candle_begin_time'] - select_coin.iloc[0]['candle_begin_time']
    total_seconds = time_during.days * 24 * 3600 + time_during.seconds
    if total_seconds == 0:
        annual_return = 0
    else:
        final_r = round(select_coin['资金曲线'].iloc[-1], 2)
        annual_return = pow(final_r, 24 * 3600 * 365 / total_seconds) - 1

    results.loc[0, '年化收益'] = str(round(annual_return, 2)) + ' 倍'
    results.loc[0, '年化收益/回撤比'] = round((annual_return) / abs(max_draw_down), 2)

    return results, select_coin





