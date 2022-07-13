#!/usr/bin/python3
# -*- coding: utf-8 -*-

import pandas as pd
import numpy  as np


# ===目标函数
# 收益/回撤比
def target_ratio(select_c):
    # ===计算最大回撤
    select_c['max2here'] = select_c['资金曲线'].expanding().max()
    # 计算到历史最高值到当日的跌幅，drowdwon
    select_c['dd2here']  = select_c['资金曲线']/select_c['max2here'] - 1
    # 计算最大回撤，以及最大回撤结束时间
    end_date, max_draw_down = tuple(select_c.sort_values(by=['dd2here']).iloc[0][['candle_begin_time', 'dd2here']])
    # 计算最后收益
    final_r  = round(select_c['资金曲线'].iloc[-1], 2)

    return final_r/(-max_draw_down)


# 年化收益/回撤比
def annual_target_ratio(select_c):
	# ===计算最大回撤
	select_c['max2here'] = select_c['资金曲线'].expanding().max()
	# 计算到历史最高值到当日的跌幅，drowdwon
	select_c['dd2here']  = select_c['资金曲线']/select_c['max2here'] - 1
	# 计算最大回撤，以及最大回撤结束时间
	end_date, max_draw_down = tuple(select_c.sort_values(by=['dd2here']).iloc[0][['candle_begin_time', 'dd2here']])
	# 计算最后收益
	annual_return = (select_c['资金曲线'].iloc[-1] / select_c['资金曲线'].iloc[0]) ** (
		'1 days 00:00:00' / (select_c['candle_begin_time'].iloc[-1] - select_c['candle_begin_time'].iloc[0]) * 365) - 1
	return annual_return/(-max_draw_down)



