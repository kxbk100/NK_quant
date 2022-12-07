import sys
import os

_ = os.path.abspath(os.path.dirname(__file__))  # 返回当前文件路径
root_path = os.path.abspath(os.path.join(_, '../'))  # 返回根目录文件夹
sys.path.append(root_path)
# from config import root_path
# from utils import reader, tools, ind

from src_backtesting.config import root_path
from src_backtesting.utils import reader, tools, ind

import math
import platform
import time
import warnings
import pandas as pd
import numpy as np
import itertools
import datetime
from joblib import Parallel, delayed
from plotly.subplots import make_subplots
import plotly.graph_objects as go
import matplotlib
import matplotlib.pyplot as plt
import matplotlib.ticker as tc
warnings.filterwarnings("ignore")

from typing import Union, List

def load_playCfg(playCfg):
    return playCfg['c_rate'][0], playCfg['hold_hour_num'][0], playCfg['select_coin_num'][0], playCfg['sell_coin_num'][0], playCfg[
        'buy_p'][0], playCfg['sell_p'][0], playCfg['leverage'][0], playCfg['long_risk_position'][0], playCfg['initial_trade_usdt'][0]
def load_othCfg(othCfg):
    return othCfg['cal_factor_type'],othCfg['hourly_details'], othCfg['select_by_hour'], othCfg['filter_before_exec'], othCfg['filter_after_exec'], othCfg[
        'start_date'], othCfg['end_date'], othCfg['factor_list1'], othCfg['factor_list2'], othCfg['trade_type'], othCfg['compound_name'],\
           othCfg['quit_symbol_filter_hour'],othCfg['p_signal_fun'],othCfg['select_offsets'],othCfg['white_list'],othCfg['black_list']
def filter_generate(direction: str = 'long', filter_factor: str = '涨跌幅max_fl_24', filter_type: str = 'value',
                    filter_value: Union[int, float, List[Union[int, float]]] = 0.2, compare_operator : str = 'lt',
                    rank_ascending: bool = False,filter_after: bool = False, weight_ratio: float=0, param: list = []) -> str:
    """
    : param direction: 过滤的方向  '多'/'long'/'df1'或 '空'/'short'/'df2'
    : param filter_factor: 过滤因子名 如 '涨跌幅max_fl_24'
    : param filter_type: 过滤方式 value/rank/pct  原始数值/排名(默认从大到小)/百分位(从小到大)
    : param filter_value: 过滤阈值 支持 int float list
    : param compare_operator： 和数值的比较关系 lt gt bt nbt lte gte bte nbte
    : param rank_ascending: True/False 控制 rank模式下的排名方向，对pct无效
    : param filter_after: False/True 是否为后置过滤
    : param weight_ratio: 被后置的币设定资金系数 0 j即是清仓
    : param inclusive:  True 闭区间 ； Flase 开区间
    : param param: [direction,filter_factor,filter_type,filter_value,compare_operator,rank_ascending] 或 [direction,filter_factor,filter_type,filter_value,compare_operator,rank_ascending,filter_after,weight_ratio] 便于链式过滤传参
    : param compare_operator 详解：
        lt, gt, lte, gte, bt, bte, nbt, nbte 是一些缩写，它们在数学和计算机科学中有特定的含义。
        lt 是 less than 的缩写，表示“小于”。
        gt 是 greater than 的缩写，表示“大于”。
        lte 是 less than or equal to 的缩写，表示“小于等于”。
        gte 是 greater than or equal to 的缩写，表示“大于等于”。
        bt 是 between 的缩写，表示“介于两者之间”。
        bte 是 between, inclusive 的缩写，表示“介于两者之间，包括两者”。
        nbt 是 not between 的缩写，表示“不介于两者之间”。
        nbte 是 not between, inclusive 的缩写，表示“不介于两者之间，但包括两者”。
    """
    if len(param) == 8:
        direction, filter_factor, filter_type, filter_value, compare_operator, rank_ascending, filter_after, weight_ratio = param
    elif len(param) == 6:
        direction, filter_factor, filter_type, filter_value, compare_operator,rank_ascending = param
    elif len(param) == 0:
        pass
    else:
        raise ValueError('Wrong param length!')
    direction_map = {'多': 'df1', 'long': 'df1', 'df1': 'df1',
                     '空': 'df2', 'short': 'df2', 'df2': 'df2'}
    dfx = direction_map.get(direction)
    # print(compare_operator)

    if dfx is None:
        raise ValueError('Wrong direction!')

    assert filter_type in ['value', 'rank', 'pct']
    assert compare_operator in ['lt','lte','gt','gte','bt','bte','nbt','nbte']
    assert type(filter_factor) == str
    assert rank_ascending in [True, False]
    if compare_operator.endswith('e'):
        # print('采用闭区间')
        inclusive = True
    else:
        inclusive = False
    if filter_type == 'pct': rank_ascending = True
    if compare_operator in ['lt', 'lte', 'gt', 'gte']:
        if filter_type == 'rank':
            assert type(filter_value) == int
            use_pct = False
        elif filter_type == 'pct':
            assert 0 <= filter_value <= 1
            use_pct = True
        else:
            assert type(filter_value) in [float, int]
    else:
        assert type(filter_value) == list
        assert filter_value[0] < filter_value[1]
        if filter_type == 'rank':
            assert type(filter_value[0]) == int
            assert type(filter_value[1]) == int
            use_pct = False
        elif filter_type == 'pct':
            assert 0 <= filter_value[0] <= 1
            assert 0 <= filter_value[1] <= 1
            use_pct = True
    if type(filter_value) != list:
        if compare_operator in ['lt','lte']:
            filter_value = [-1e100, filter_value]
        elif compare_operator in ['gt','gte']:
            filter_value = [filter_value, 1e100]
    if compare_operator[:2] == 'nb':
        inclusive = not inclusive
        reverse = '~'
    else:
        reverse = ''
    if filter_type == 'value':
        rank_str = f"filter_factor = ['{filter_factor}'][0]"
    else:
        rank_str = f"{dfx}[f'{filter_factor}_rank'] = {dfx}.groupby('candle_begin_time')['{filter_factor}'].rank(method='first', pct={use_pct}, ascending={rank_ascending})"
        filter_factor = f'{filter_factor}_rank'
    left, right = filter_value
    if not filter_after:
        compare_str = f"{dfx} = {dfx}[{reverse}{dfx}[f'{filter_factor}'].between({left},{right},inclusive={inclusive})]"
    else:
        compare_str = f"{dfx}.loc[{reverse}{dfx}[f'{filter_factor}'].between({left},{right},inclusive={inclusive}),'weight_ratio'] = {weight_ratio}"
    filter_str = f"""{rank_str}\n{compare_str}"""
    return filter_str
def parallel_filter_handle(filter_before_exec):
    '''
    将默认的串联过滤转化为并联过滤，只使用于filter_generate生成的过滤逻辑,后置过滤不适用，默认并联
    '''
    series_filter_list = []
    for content in filter_before_exec:
        series_filter_list += content.split('\n')
    parallel_filter_list = series_filter_list[::2] + series_filter_list[1::2]
    return parallel_filter_list


def np_select_by_hour(run_time, hold_hour_num, select_coin_num,sell_coin_num,arr_data,quit_blacK_symbol_list):
    arr_list_long, arr_list_short,long_weight_array, short_weight_array = arr_data
    ll = []
    # print(quit_blacK_symbol_list)
    for temp in arr_list_long[run_time:hold_hour_num+run_time]:
        temp = temp.copy()
        if quit_blacK_symbol_list:
            raw_len = len(temp)
            for quit_symbol in quit_blacK_symbol_list:
                temp = temp[temp[:, 1] != quit_symbol]
        temp[:, 5] = temp[:, 5].argsort().argsort() + 1
        ll.append(temp[np.where(temp[:, 5] <= select_coin_num)])
    select_coin_long = np.vstack(ll)

    ll = []
    for temp in arr_list_short[run_time:hold_hour_num+run_time]:
        temp = temp.copy()
        if quit_blacK_symbol_list:
            for quit_symbol in quit_blacK_symbol_list:
                temp = temp[temp[:, 1] != quit_symbol]
        temp[:, 5] = (-temp[:, 5]).argsort().argsort() + 1
        ll.append(temp[np.where(temp[:, 5] <= sell_coin_num)])
    select_coin_short = np.vstack(ll)
    for rank, w in enumerate(long_weight_array):
        select_coin_long[:, 3] = np.where(select_coin_long[:, 5] == rank + 1, w, select_coin_long[:, 3])
    for rank, w in enumerate(short_weight_array):
        select_coin_short[:, 3] = np.where(select_coin_short[:, 5] == rank + 1, w, select_coin_short[:, 3])

    select_coin_long[:, 3] = select_coin_long[:, 3] * select_coin_long[:, 6]
    select_coin_short[:, 3] = select_coin_short[:, 3] * select_coin_short[:, 6]
    return select_coin_long[:, 1:5], select_coin_short[:, 1:5]



def get_select_data(playCfg,run_time,all_trade_usdt,arr_data,quit_blacK_symbol_list,cache_data=[]):
    # print(run_time)
    c_rate, hold_hour_num, select_coin_num, sell_coin_num, buy_p, sell_p, leverage, long_risk_position, initial_trade_usdt = load_playCfg(playCfg)

    if len(arr_data)>0:
        # 实时从交易所拿数据计算因子即实现仿盘功能
        # print(run_time,'逐时选币')
        select_long_, select_short_ = np_select_by_hour(run_time, hold_hour_num, select_coin_num,sell_coin_num, arr_data,quit_blacK_symbol_list)
    else:
        select_long, select_short = cache_data
        select_long_ = select_long[(run_time) * select_coin_num :(run_time + hold_hour_num) * select_coin_num ]
        select_short_ = select_short[(run_time) * sell_coin_num :(run_time + hold_hour_num) * sell_coin_num ]
    # 计算多头风险暴露后的 资金分配
    select_long_[:, 3] = all_trade_usdt / hold_hour_num / 2 * select_long_[:, 2] * (1 + long_risk_position)
    select_short_[:, 3] = all_trade_usdt / hold_hour_num / 2 * select_short_[:, 2] * (1 - long_risk_position)
    # print(select_long_)
    # print(select_short_)
    # exit()
    return select_long_, select_short_

def trade_symbol_info(run_time, all_trade_usdt, symbol_info, select_long_, select_short_, min_qtys, open_prices,
                       close_prices,c_rate):
    next_run_time = run_time + 1
    # 计算实际下单量
    target_amount_long = select_long_[:, 3] / select_long_[:, 1]
    target_amount_short = -select_short_[:, 3] / select_short_[:, 1]

    for i in range(select_long_.shape[0]):
        symbol = int(select_long_[i, 0])
        symbol_info[symbol, 2] += target_amount_long[i]
    for i in range(select_short_.shape[0]):
        symbol = int(select_short_[i, 0])
        symbol_info[symbol, 2] += target_amount_short[i]

    symbol_info[:, 3] = symbol_info[:, 2] - symbol_info[:, 0]
    # 下单量精度修正
    for symbol in range(symbol_info.shape[0]):
        min_qty = min_qtys[symbol]
        symbol_info[symbol, 3] = np.floor(symbol_info[symbol, 3] * (10 ** min_qty)) / (10 ** min_qty)
    symbol_info[:, 11] = close_prices[run_time]
    symbol_info[:, 5] = symbol_info[:, 3] * symbol_info[:, 11]
    # 处理小于5 和reduce_only 问题
    symbol_info[:, 3] = np.where((np.abs(symbol_info[:, 5]) < 5) & (symbol_info[:, 2] != 0), 0, symbol_info[:, 3])
    symbol_info[:, 5] = symbol_info[:, 3] * symbol_info[:, 11]

    # K线开始，交易对持仓账户影响
    symbol_info[:, 4] = open_prices[run_time]
    symbol_info[:, 3] = np.where(np.isnan(symbol_info[:, 3]), 0, symbol_info[:, 3])
    symbol_info[:, 4] = np.where(np.isnan(symbol_info[:, 4]), 0, symbol_info[:, 4])
    symbol_info[:, 6] = symbol_info[:, 4] * np.abs(symbol_info[:, 3]) * c_rate
    symbol_info[:, 7] = np.where(symbol_info[:, 0] > symbol_info[:, 3], symbol_info[:, 3], symbol_info[:, 0])

    symbol_info[:, 7] = np.where(np.sign(symbol_info[:, 0]) == np.sign(symbol_info[:, 3]), 0,
                                 np.where(np.abs(symbol_info[:, 0]) > np.abs(symbol_info[:, 3]), symbol_info[:, 3],
                                          symbol_info[:, 0]))

    symbol_info[:, 8] = symbol_info[:, 0] + symbol_info[:, 3]

    symbol_info[:, 9] = np.abs(symbol_info[:, 7]) * (symbol_info[:, 4] - symbol_info[:, 1]) * np.sign(symbol_info[:, 0])

    symbol_info[:, 10] = np.where(symbol_info[:, 7] == 0,
                                  (symbol_info[:, 1] * symbol_info[:, 0] + symbol_info[:, 3] * symbol_info[:, 4]) / (
                                              symbol_info[:, 0] + symbol_info[:, 3]),
                                  np.where(np.abs(symbol_info[:, 0]) > np.abs(symbol_info[:, 3]), symbol_info[:, 1],
                                           symbol_info[:, 4]))
    # 记录月化换手率 和 K线结束持仓状态
    monthly_turnover_rate = np.nansum(np.abs(symbol_info[:, 5])) / all_trade_usdt / 2 * 24 * 30.4

    return symbol_info, monthly_turnover_rate
def update_symbol_info(run_time, symbol_info, close_prices):
    next_run_time = run_time + 1
    # K线结束，close对持仓账户影响
    symbol_info[:, 11] = close_prices[next_run_time]
    symbol_info[:, 12] = symbol_info[:, 8] * (symbol_info[:, 11] - symbol_info[:, 10])
    # 计算已实现盈亏 未实现盈亏 交易手续费
    totalRealizedProfit = np.nansum(symbol_info[:, 9])
    totalUnrealizedProfit = np.nansum(symbol_info[:, 12])
    commission = -np.nansum(symbol_info[:, 6])
    symbol_info_ = symbol_info.copy()

    symbol_info[:, 8] = np.where(np.abs(symbol_info[:, 8]) * symbol_info[:, 11] < 1, 0, symbol_info[:, 8])
    # K线结束 重置持仓账户
    symbol_info[:, 0] = symbol_info[:, 8]
    symbol_info[:, 1] = symbol_info[:, 10]
    symbol_info[:, 2:] = 0
    symbol_info[:, 1] = np.where(np.isnan(symbol_info[:, 1]), 0, symbol_info[:, 1])
    return totalRealizedProfit, totalUnrealizedProfit, commission, symbol_info_, symbol_info
def timming_fun(signal_fun,curve,trade_ratio_limit=0.1):
    if signal_fun is None: return 1
    trade_ratio = signal_fun(curve)
    return max(trade_ratio,trade_ratio_limit)
def neutral_playback(playCfg,N,p_signal_fun,select_long, select_short, account, symbol_info, open_prices, close_prices, quit_arry,min_qtys,arr_data):

    c_rate, hold_hour_num, select_coin_num, sell_coin_num, buy_p, sell_p, leverage, long_risk_position, initial_trade_usdt = load_playCfg(playCfg)
    month_turnover_rate_list = []
    symbol_info_list = []
    ls_list = [[0., 0.]]
    hold_symbol_list = []
    quit_blacK_symbol_list = []
    account[0, 0] = initial_trade_usdt
    account[0, 1] = initial_trade_usdt
    account[0, 5] = np.inf
    curve = account[:, 0].copy()
    curve[0] = 1
    cache_data = [select_long, select_short]
    for run_time in range(N):
        next_run_time = run_time + 1
        curve_ = curve[:next_run_time]
        trade_ratio = timming_fun(p_signal_fun, curve_)
        all_trade_usdt = account[run_time, 0] * leverage * trade_ratio
        # 获取该时刻选币数据和多头风险暴露后的资金分配
        select_long_, select_short_ = get_select_data(playCfg,run_time,all_trade_usdt,arr_data,quit_blacK_symbol_list,cache_data=cache_data)
        # 退市币最后一根有效K强制清仓
        if len(arr_data) > 0:
            if len(quit_arry) > 0:
                clear_arr = quit_arry[quit_arry[:,0] == run_time]
                if len(clear_arr) > 0:
                    for quit_symbol in clear_arr[:,1]:
                        select_long_[:,3] = np.where([select_long_[:,0] == quit_symbol], 0 , select_long_[:,3])
                        select_short_[:,3] = np.where([select_short_[:,0] == quit_symbol], 0 , select_short_[:,3])
                        quit_blacK_symbol_list.append(quit_symbol)
                        print(quit_symbol,':即将下架，马上获取不到K线数据,执行清仓并拉入黑名单')

        # 开盘交易 symbol_info
        symbol_info, monthly_turnover_rate = trade_symbol_info(run_time, all_trade_usdt, symbol_info, select_long_, select_short_, min_qtys, open_prices, close_prices,c_rate)
        month_turnover_rate_list.append(monthly_turnover_rate)

        # 任意时点更新 symbol_info
        totalRealizedProfit, totalUnrealizedProfit, commission, symbol_info_, symbol_info = update_symbol_info(run_time, symbol_info, close_prices)


        long_value = np.nansum(np.where(symbol_info_[:, 8] > 0, symbol_info_[:, 8] * symbol_info_[:, 11], 0))
        short_value = np.nansum(np.where(symbol_info_[:, 8] < 0, -symbol_info_[:, 8] * symbol_info_[:, 11], 0))

        ls_list.append([long_value, short_value])
        symbol_info_[:, 8] = np.where(np.abs(symbol_info_[:, 8]) * symbol_info_[:, 11] < 0.001 * all_trade_usdt, 0, symbol_info_[:, 8])
        symbol_info_list.append(symbol_info_)

        hold_symbol_list.append([np.argwhere((symbol_info_[:, 8] > 0) )[:, 0],np.argwhere((symbol_info_[:, 8] < 0) )[:,0]])
        # 更新币安账户
        account[next_run_time, 2] = totalRealizedProfit
        account[next_run_time, 3] = totalUnrealizedProfit
        account[next_run_time, 4] = commission
        account[next_run_time, 0] = account[run_time, 0] + account[next_run_time, 2] + account[next_run_time, 4]
        account[next_run_time, 1] = account[next_run_time, 0] + account[next_run_time, 3]
        account[next_run_time, 5] = account[next_run_time, 1] / (np.abs(np.nansum((symbol_info_[:, 8] * symbol_info_[:, 11])))+ 1e-8)
        curve[next_run_time] = curve[run_time] * (1 + (account[next_run_time,1] / account[run_time,1] -1) / leverage / trade_ratio)
    return account, month_turnover_rate_list, symbol_info_list, ls_list, hold_symbol_list


def cal_hourly_details(replace_symbol_to_int,time_index, i, data):
    symbol_info = pd.DataFrame(data, columns=['当前持仓量', '开仓价格', '目标下单量', '实际下单量', 'avg_price', '实际下单资金', '手续费', '已实现仓位',
                                              '交易后持仓量', '已实现盈亏', '新开仓价格', 'close', '未实现盈亏'],
                               index=replace_symbol_to_int.keys())
    # 各小时持仓详情记录
    display_df = symbol_info[['交易后持仓量', 'close', '未实现盈亏','开仓价格']]
    display_df = display_df[display_df['交易后持仓量'] != 0]

    display_df['direction'] = np.sign(display_df['交易后持仓量'])

    display_df['notional'] = (display_df['交易后持仓量'] * display_df['close']).abs()
    display_df['持仓均价'] = display_df['开仓价格'] #/ np.abs(display_df['交易后持仓量'])

    # 持仓市值占比
    display_df['national_p'] = display_df['notional'] / display_df['notional'].sum()
    # 总市值盈亏贡献占比
    display_df['未实现盈亏_p'] = display_df['未实现盈亏'] / display_df['notional'].sum()
    # 盈亏贡献占比
    display_df.loc[display_df['未实现盈亏_p'] < 0, 'loss_profit_p'] = display_df.loc[display_df['未实现盈亏_p'] < 0, '未实现盈亏'] / \
                                                                 display_df.loc[
                                                                     display_df['未实现盈亏_p'] < 0, '未实现盈亏'].sum()
    display_df.loc[display_df['未实现盈亏_p'] >= 0, 'win_profit_p'] = display_df.loc[display_df['未实现盈亏_p'] >= 0, '未实现盈亏'] / \
                                                                 display_df.loc[
                                                                     display_df['未实现盈亏_p'] >= 0, '未实现盈亏'].sum()
    display_df['未实现盈亏_p'] = display_df['未实现盈亏_p'] / display_df['national_p']
    display_df['win_loss_distribute'] = display_df[['loss_profit_p', 'win_profit_p']].max(axis=1)
    # 持仓市值排序
    display_df['national_rank'] = display_df['national_p'].rank(ascending=False)
    display_df.index.name = 'symbol'

    display_df.sort_values('national_rank', inplace=True)

    display_df = display_df[
        ['交易后持仓量', 'direction', '持仓均价','close','notional', 'national_p', '未实现盈亏','未实现盈亏_p', 'win_loss_distribute', 'national_rank']]

    display_df.columns = ['持仓数量', '持仓方向', '持仓均价','币种现价','持仓市值', '持仓市值占比%',  '未实现盈亏','未实现盈亏占市值比%', '盈利亏损贡献度%', '持仓市值排名']

    display_df[['持仓市值占比%', '未实现盈亏占市值比%', '盈利亏损贡献度%']] = display_df[['持仓市值占比%', '未实现盈亏占市值比%', '盈利亏损贡献度%']] * 100

    display_df[['持仓市值', '持仓市值占比%', '未实现盈亏','未实现盈亏占市值比%', '盈利亏损贡献度%']] = display_df[
        ['持仓市值', '持仓市值占比%', '未实现盈亏','未实现盈亏占市值比%', '盈利亏损贡献度%']].round(2)

    display_df['candle_begin_time'] = time_index[i + 1]
    display_df = display_df.reset_index()

    order_df = symbol_info[['当前持仓量', '实际下单量', 'avg_price']].reset_index()
    order_df.columns = ['symbol', '当前持仓量', '实际下单量', '理想开仓均价']
    order_df = order_df[order_df['实际下单量'] != 0]
    order_df['candle_begin_time'] = time_index[i] + datetime.timedelta(minutes=1)
    return display_df, order_df



def freestep_evaluate(ls_df,long_hold,short_hold,month_turnover_rate_list=[0],compound_name='策略评价'):
    # 计算统计指标
    month_turnover_rate = np.nanmean(month_turnover_rate_list)
    key = compound_name
    results = pd.DataFrame()
    curve = ls_df['资金曲线'].to_frame(compound_name)
    curve.index.name = 'candle_begin_time'
    curve_ = curve.copy()
    curve.reset_index(inplace=True)
    curve['本周期多空涨跌幅'] = curve[key].pct_change().fillna(0)
    # 累积净值
    results.loc[key, '累积净值'] = round(curve[key].iloc[-1], 3)
    # 计算当日之前的资金曲线的最高点
    curve['max2here'] = curve[key].expanding().max()
    # 计算到历史最高值到当日的跌幅，drowdwon
    curve['dd2here'] = curve[key] / curve['max2here'] - 1
    # 计算最大回撤，以及最大回撤结束时间
    end_date, max_draw_down = tuple(curve.sort_values(by=['dd2here']).iloc[0][['candle_begin_time', 'dd2here']])
    # 计算最大回撤开始时间
    start_date = curve[curve['candle_begin_time'] <= end_date].sort_values(by=key, ascending=False).iloc[0][
        'candle_begin_time']
    # 将无关的变量删除
    curve.drop(['max2here', 'dd2here'], axis=1, inplace=True)
    results.loc[key, '最大回撤'] = format(max_draw_down, '.2%')
    results.loc[key, '最大回撤开始时间'] = str(start_date)
    results.loc[key, '最大回撤结束时间'] = str(end_date)
    # ===统计每个周期
    results.loc[key, '盈利周期数'] = len(curve.loc[curve['本周期多空涨跌幅'] > 0])  # 盈利笔数
    results.loc[key, '亏损周期数'] = len(curve.loc[curve['本周期多空涨跌幅'] <= 0])  # 亏损笔数
    results.loc[key, '胜率'] = format(results.loc[key, '盈利周期数'] / len(curve), '.2%')  # 胜率
    results.loc[key, '每周期平均收益'] = format(curve['本周期多空涨跌幅'].mean(), '.3%')  # 每笔交易平均盈亏
    if curve.loc[curve['本周期多空涨跌幅'] <= 0]['本周期多空涨跌幅'].mean() != 0:
        results.loc[key, '盈亏收益比'] = round(curve.loc[curve['本周期多空涨跌幅'] > 0]['本周期多空涨跌幅'].mean() / \
                                          curve.loc[curve['本周期多空涨跌幅'] <= 0]['本周期多空涨跌幅'].mean() * (-1), 2)  # 盈亏比
    else:
        results.loc[key, '盈亏收益比'] = np.nan
    results.loc[key, '单周期最大盈利'] = format(curve['本周期多空涨跌幅'].max(), '.2%')  # 单笔最大盈利
    results.loc[key, '单周期大亏损'] = format(curve['本周期多空涨跌幅'].min(), '.2%')  # 单笔最大亏损
    # ===连续盈利亏损
    results.loc[key, '最大连续盈利周期数'] = max(
        [len(list(v)) for k, v in itertools.groupby(np.where(curve['本周期多空涨跌幅'] > 0, 1, np.nan))])  # 最大连续盈利次数
    results.loc[key, '最大连续亏损周期数'] = max(
        [len(list(v)) for k, v in itertools.groupby(np.where(curve['本周期多空涨跌幅'] <= 0, 1, np.nan))])  # 最大连续亏损次数
    results.loc[key, '月换手率'] = month_turnover_rate
    # ===每年、每月收益率
    curve.set_index('candle_begin_time', inplace=True)
    #     year_return = curve[['本周期多空涨跌幅']].resample(rule='A').apply(lambda x: (1 + x).prod() - 1)
    #     month_return = curve[['本周期多空涨跌幅']].resample(rule='M').apply(lambda x: (1 + x).prod() - 1)
    #     year_return.columns=[key]
    #     month_return.columns=[key]

    # 计算相对年化 最大回撤 信息系数 波动率
    result_stats = pd.DataFrame(index=['年化收益','月化收益', '月信息比', '月化波动'],
                                columns=curve_.columns)
    result_stats.loc['年化收益'][:] = np.power(curve_.iloc[-1],  365 * 24 / (curve_.shape[0] - 1)) - 1
    result_stats.loc['月化收益'][:] = np.power(curve_.iloc[-1], 30.4 * 24 / (curve_.shape[0] - 1)) - 1
    result_stats.loc['月化波动'][:] = curve_.pct_change().dropna().apply(lambda x: x.std() * np.sqrt(30.5 * 24))
    result_stats.loc['月信息比'][:] = (result_stats.loc['月化收益'][:] / result_stats.loc['月化波动'][:])
    result_stats = result_stats.astype('float32').round(3)

    data = multi_list_merge([result_stats.T, results])
    data['月化收益回撤比'] = data['月化收益'] / abs(data['最大回撤'].str[:-1].astype('float32')) * 100

    data = data[['累积净值', '年化收益','月化收益', '月信息比', '月化波动', '月换手率', '月化收益回撤比',  '最大回撤', '最大回撤开始时间', '最大回撤结束时间', '盈利周期数',
                 '亏损周期数', '胜率', '每周期平均收益', '盈亏收益比', '单周期最大盈利', '单周期大亏损', '最大连续盈利周期数',
                 '最大连续亏损周期数']]

    curve = ls_df[['资金曲线', '多头占比', '空头占比']]

    curve['long_hold_symbol'] = ' '
    curve['short_hold_symbol'] = ' '

    curve['long_hold_symbol'].iloc[1:] = long_hold.values
    curve['short_hold_symbol'].iloc[1:] = short_hold.values
    return data, curve

def neutral_strategy_playback(
                            playCfg,
                            p_signal_fun,
                            start_date,
                            end_date,
                            symbols_data,
                            arr_data,
                            quit_arry,
                            all_symbol_list,
                            replace_symbol_to_int,
                            replace_symbol_to_int_,
                            select_long,
                            select_short,
                            compound_name='中性回放',
                            min_marginRatio=0.01,
                            hourly_details=False):
    # 载入配置
    c_rate, hold_hour_num, select_coin_num, sell_coin_num, buy_p, sell_p, leverage, long_risk_position, initial_trade_usdt = load_playCfg(playCfg)

    # 读取币种精度数据
    min_qty_path = os.path.join(root_path, 'data', '最小下单量.csv')
    min_qty_df = pd.read_csv(min_qty_path, encoding='gbk')
    min_qty_df['合约'] = min_qty_df['合约'].str.replace('-', '')
    min_qty_df = pd.DataFrame(
        all_symbol_list,
        columns=['合约']).merge(
            min_qty_df,
            on=['合约'],
        how='left')
    min_qty_df['最小下单量'].fillna(min_qty_df['最小下单量'].min(), inplace=True)
    min_qty_df['最小下单量'] = min_qty_df['最小下单量'].apply(
        lambda x: int(math.log(float(x), 0.1)))
    min_qtys = min_qty_df['最小下单量'].to_numpy(dtype=np.float64)

    time_index = pd.date_range(
        start=start_date,
        end=end_date +
        datetime.timedelta(
            hours=1),
        freq='1H')
    # op cl 数据转换 为numpy
    open_price_df = symbols_data.pivot_table(
        index=['candle_begin_time'],
        columns=['symbol'],
        values=['avg_price'])
    close_price_df = symbols_data.pivot_table(
        index=['candle_begin_time'],
        columns=['symbol'],
        values=['close'])
    open_price_df = open_price_df.loc[start_date:end_date]
    close_price_df = close_price_df.loc[start_date - datetime.timedelta(hours=1):end_date]
    # 和内部规则恰好一致
    # open_price_df = open_price_df.rename(columns=replace_symbol_to_int)
    # close_price_df = close_price_df.rename(columns=replace_symbol_to_int)
    open_prices = open_price_df.to_numpy(dtype=np.float64)
    close_prices = close_price_df.to_numpy(dtype=np.float64)

    N = pd.date_range(start=start_date, end=end_date, freq='1H').shape[0]
    # 初始化 币安钱包账户
    account = np.zeros((N + 1, 6), dtype=np.float64)
    # 初始化 合约持仓账户
    symbol_info = np.zeros((len(all_symbol_list), 13), dtype=np.float64)
    # 选币模式
    account, month_turnover_rate_list, symbol_info_list, ls_list, hold_symbol_list = neutral_playback(playCfg,N, p_signal_fun, select_long, select_short, account, symbol_info, open_prices, close_prices,quit_arry, min_qtys,arr_data)
    account_df = pd.DataFrame(
        account,
        index=time_index,
        columns=[
            'totalWalletBalance',
            'totalMarginBalance',
            'totalRealizedProfit',
            'totalUnRealizedProfit',
            'commission',
            'marginRatio'])
    # 爆仓处理 min_marginRatio = 0.01
    if account_df['marginRatio'].min() < min_marginRatio:
        temp = account_df['marginRatio'].min()
        print(f'保证金比例：{temp} 小于 {min_marginRatio},恭喜您爆仓了！')
        ind = account_df[account_df['marginRatio'] < min_marginRatio].index[0]
        account_df.loc[ind:, ['totalWalletBalance', 'totalMarginBalance']] = 1e-8
    df = pd.DataFrame([] + [x[0] for x in hold_symbol_list])
    df = df.replace(replace_symbol_to_int_)
    df.fillna('', inplace=True)
    df = df + ' '
    if df.shape[1] == 0:
        df[0] = ''
        long_hold = df[0]
    else:
        long_hold = df.sum(axis=1).str.strip()
    df = pd.DataFrame([] + [x[1] for x in hold_symbol_list])
    df = df.replace(replace_symbol_to_int_)
    df.fillna('', inplace=True)
    df = df + ' '
    if df.shape[1] == 0:
        df[0] = ''
        short_hold = df[0]
    else:
        short_hold = df.sum(axis=1).str.strip()

    if hourly_details:
        res_list = Parallel(
            n_jobs=-2,
            verbose=0)(
            delayed(cal_hourly_details)(
                replace_symbol_to_int,
                time_index,
                i,
                data) for i,
            data in enumerate(symbol_info_list))
        display_list = [x[0] for x in res_list]
        order_df_list = [x[1] for x in res_list]
        display_df = pd.concat(display_list)
        display_df = display_df.set_index(['candle_begin_time', 'symbol'])
        # display_df = display_df[display_df['持仓市值'] >= 1]
        order_df = pd.concat(order_df_list)
        order_df = order_df.set_index(['candle_begin_time', 'symbol'])
    else:
        display_df = pd.DataFrame()
        order_df = pd.DataFrame()

    ls_df = pd.DataFrame((np.array(ls_list).transpose(
    ) / account[:, 1]).transpose(), columns=['多头占比', '空头占比'], index=time_index).round(4)

    ls_df['资金曲线'] = account_df['totalMarginBalance'] / \
        account_df['totalMarginBalance'].iloc[0]
    # 策略评价
    res, curve = freestep_evaluate(ls_df, long_hold, short_hold,
                                   month_turnover_rate_list=month_turnover_rate_list, compound_name=compound_name)
    cmmmission_loss = (1 - account_df['commission'] / account_df['totalMarginBalance']).cumprod().iloc[-1] - 1

    res['交易费率'] = c_rate * 10000
    res['leverage'] = leverage
    res['手续费磨损净值'] = cmmmission_loss * res['累积净值'].iloc[0]
    final_trade_usdt = round(account_df.iloc[-1]['totalMarginBalance'],2)
    cmmmission_sum = round(account_df['commission'].sum(),2)
    # 取出需要调整顺序的列数据'D'
    d = res.pop('手续费磨损净值')
    # 利用insert方法插入取出的数据列到指定位置
    res.insert(1, '手续费磨损净值', d)
    print(f'初始投入资产：{initial_trade_usdt} U,最终账户资产：{final_trade_usdt} U, 共支付手续费：{cmmmission_sum} U')
    return res, curve, account_df, display_df, order_df
def multi_list_merge(df_list,on=None,how='inner'):
    if len(df_list)==1:
        return df_list[0]
    if on == None:
        for i in range(len(df_list)-1):
            if i==0:
                merge_df = pd.merge(df_list[0],df_list[1],left_index=True,right_index=True,how=how)
            else:
                merge_df = merge_df.merge(df_list[i+1],left_index=True,right_index=True,how=how)
    else:
        for i in range(len(df_list)-1):
            if i==0:
                merge_df = pd.merge(df_list[0],df_list[1],on=on,how=how)
            else:
                merge_df = merge_df.merge(df_list[i+1],on=on,how=how)
    return merge_df
def w_log(p, coins_num):
    array = np.arange(1, coins_num + 1)
    if p > 0:
        array = np.log(array + p)
    else:
        array = np.full(coins_num, 1)
    weight_array = array[::-1] / array.sum()
    return weight_array
# 横截面
def cal_factor_by_cross(df, factor_list1, factor_list2, pct_enable=False):
    feature_list = tools.convert_to_feature(factor_list1 + factor_list2)
    # ===数据预处理
    df = df.set_index(['candle_begin_time', 'symbol']).sort_index()
    # 横截面排名
    df[feature_list] = df.groupby('candle_begin_time')[feature_list].apply(
        lambda x: x.rank(pct=pct_enable, ascending=True))
    df[feature_list] = df.groupby('candle_begin_time')[
        feature_list].apply(lambda x: x.fillna(x.median()))
    df.reset_index(inplace=True)

    df = tools.cal_factor_by_verical(df, factor_list1, factor_tag='多头因子')
    df = tools.cal_factor_by_verical(df, factor_list2, factor_tag='空头因子')

    return df
# 纵截面
def cal_factor_by_verical(df, factor_list1, factor_list2):
    df = tools.cal_factor_by_verical(df, factor_list1, factor_tag='多头因子')
    df = tools.cal_factor_by_verical(df, factor_list2, factor_tag='空头因子')
    return df
# np选币
def np_gen_selected(df, base_index, filter_before_exec,filter_after_exec,select_by_hour,playCfg,select_offsets,white_list,black_list,replace_symbol_to_int):
    import copy
    c_rate, hold_hour_num, select_coin_num, sell_coin_num, buy_p, sell_p, leverage, long_risk_position, initial_trade_usdt = load_playCfg(playCfg)

    df['weight'] = 0
    df['assign_usdt'] = 0
    df['weight_ratio'] = 1
    df['time'] = df['candle_begin_time'].copy()
    df['candle_begin_time'] = pd.to_numeric(df['candle_begin_time'])
    base_time = pd.to_numeric(pd.Series(pd.to_datetime('20170101'))).iloc[0]
    df['offset'] = (df['candle_begin_time'] - base_time) / 3.6e12 % hold_hour_num

    df1 = df.copy()
    df2 = df.copy()
    time_length = len(df['time'].unique())

    # 前置过滤
    df1, df2 = filter_before(df1, df2, filter_before_exec, white_list, black_list,replace_symbol_to_int)
    time_length1 = len(df1['time'].unique())
    time_length2 = len(df2['time'].unique())
    filter_miss = False
    # print(df['time'].unique())
    # print(df1['time'].unique())
    if time_length != np.mean([time_length1, time_length2]):
        print('由于过滤因子异常或过滤条件苛刻，导致某些小时合约数量不够，进入容错选币算法，数量不够的小时将空仓，耗时很长。建议检查过滤条件，多空不平衡玩法可以在后置过滤完成')
        # print('多头缺失日期:',[x for x in df1['time'].unique() if x not in df['time'].unique()])
        # print('空头缺失日期:',[x for x in df2['time'].unique() if x not in df['time'].unique()])
        print('多头缺失日期:',set(df['time'].unique()) - set(df1['time'].unique()))
        print('空头缺失日期:',set(df['time'].unique()) - set(df2['time'].unique()))
        filter_miss = True

    # 后置过滤前置化
    df1, df2 = filter_after(df1, df2, filter_after_exec)


    # print(df1.groupby('candle_begin_time').size().min())
    # print(df2.groupby('candle_begin_time').size().min())
    # exit()
    # 指定offset
    long_select_offset, short_select_offset = select_offsets
    if long_select_offset:
        df1.loc[df1['offset'].isin(long_select_offset), 'weight_ratio'] *= (hold_hour_num / len(long_select_offset))
        df1.loc[~df1['offset'].isin(long_select_offset), 'weight_ratio'] = 0
    if short_select_offset:
        df2.loc[df2['offset'].isin(short_select_offset), 'weight_ratio'] *= (hold_hour_num / len(short_select_offset))
        df2.loc[~df2['offset'].isin(short_select_offset), 'weight_ratio'] = 0

    # 权重计算
    long_weight_array = w_log(p=buy_p, coins_num=select_coin_num)
    short_weight_array = w_log(p=sell_p, coins_num=sell_coin_num)
    arr1 = df1[['candle_begin_time', 'symbol', 'close', 'weight', 'assign_usdt', '多头因子', 'weight_ratio']].to_numpy(
        dtype='float64')
    arr2 = df2[['candle_begin_time', 'symbol', 'close', 'weight', 'assign_usdt', '空头因子', 'weight_ratio']].to_numpy(
        dtype='float64')

    arr = arr1.copy()
    arr = np.split(arr, np.unique(arr[:, 0], return_index=True)[1][1:])
    arr_list_long = copy.deepcopy(arr)
    ll = []
    for temp in arr:
        temp[:, 5] = temp[:, 5].argsort().argsort() + 1
        ll.append(temp[np.where(temp[:, 5] <= select_coin_num)])
    select_coin_long = np.vstack(ll)

    arr = arr2.copy()
    arr = np.split(arr, np.unique(arr[:, 0], return_index=True)[1][1:])
    arr_list_short = copy.deepcopy(arr)

    ll = []
    for temp in arr:
        temp[:, 5] = (-temp[:, 5]).argsort().argsort() + 1
        ll.append(temp[np.where(temp[:, 5] <= sell_coin_num)])
    select_coin_short = np.vstack(ll)

    boll1 = select_coin_long.shape[0] != len(base_index) * select_coin_num
    boll2 = select_coin_short.shape[0] != len(base_index) * sell_coin_num
    if boll1 | boll2:
        if not filter_miss:
            print('由于过滤后或日期范围内合约数量不够，进入容错选币算法，耗时很长\n')
            print('建议选2币的起始日期在2020年1月10日之后,3币2月1日之后，10币3月3日之后')

        all_arr = df[['candle_begin_time', 'symbol', 'close', 'weight', 'assign_usdt', '空头因子', 'weight_ratio']].to_numpy(
            dtype='float64')
        arr_list_long = []
        arr_list_short = []
        ll_long = []
        ll_short = []
        for cat in np.unique(all_arr[:, 0]):
            long = arr1[arr1[:,0] == cat]
            boll1 = long.shape[0] >= select_coin_num
            short = arr1[arr1[:,0] == cat]
            boll2 = short.shape[0] >= sell_coin_num
            if boll1 and boll2:
                arr_list_long.append(long.copy())
                arr_list_short.append(short.copy())
                long[:, 5] = long[:, 5].argsort().argsort() + 1
                long = long[np.where(long[:, 5] <= select_coin_num)]
                ll_long.append(long)
                short[:, 5] = (-short[:, 5]).argsort().argsort() + 1
                short = short[np.where(short[:, 5] <= sell_coin_num)]
                ll_short.append(short)
            else:
                fillarr = all_arr[all_arr[:, 0] == cat]
                fillarr = np.vstack([fillarr[:select_coin_num],fillarr[:sell_coin_num]])
                fillarr[:, 6] = 0
                arr_list_long.append(fillarr)
                arr_list_short.append(fillarr)
                ll_long.append(fillarr[:select_coin_num])
                ll_short.append(fillarr[:sell_coin_num])

        select_coin_long = np.vstack(ll_long)
        select_coin_short = np.vstack(ll_short)

    boll1 = select_coin_long.shape[0] == len(base_index) * select_coin_num
    boll2 = select_coin_short.shape[0] == len(base_index) * sell_coin_num
    assert boll1 & boll2

    for rank, w in enumerate(long_weight_array):
        select_coin_long[:, 3] = np.where(select_coin_long[:, 5] == rank + 1, w, select_coin_long[:, 3])
    for rank, w in enumerate(short_weight_array):
        select_coin_short[:, 3] = np.where(select_coin_short[:, 5] == rank + 1, w, select_coin_short[:, 3])

    select_coin_long[:, 3] = select_coin_long[:, 3] * select_coin_long[:, 6]
    select_coin_short[:, 3] = select_coin_short[:, 3] * select_coin_short[:, 6]
    if not select_by_hour:
        arr_data = []
    else:
        arr_data = [arr_list_long, arr_list_short, long_weight_array, short_weight_array]
    return select_coin_long[:, 1:5], select_coin_short[:, 1:5], arr_data


# 前置过滤
def filter_before(df1, df2, exec_list,white_list,black_list,replace_symbol_to_int):
    # 固定黑名单与固定白名单
    long_white_list, short_white_list = white_list
    long_black_list, short_black_list = black_list
    long_white_list = [replace_symbol_to_int[k] for k in long_white_list]
    short_white_list = [replace_symbol_to_int[k] for k in short_white_list]
    long_black_list = [replace_symbol_to_int[k] for k in long_black_list]
    short_black_list = [replace_symbol_to_int[k] for k in short_black_list]
    # print(long_white_list)
    # print(short_white_list)
    if long_white_list:
        df1 = df1[df1['symbol'].isin(long_white_list)]
    if short_white_list:
        df2 = df2[df2['symbol'].isin(short_white_list)]
    if long_black_list:
        df1 = df1[~df1['symbol'].isin(long_black_list)]
    if short_black_list:
        df2 = df2[~df2['symbol'].isin(short_black_list)]

    d = {'df1': df1,'df2': df2}
    for content in exec_list:
        try:
            exec(content, globals(), d)
            df1 = d['df1']
            df2 = d['df2']
        except IndentationError as e:
            raise ValueError(f'{e}:', '请删掉过滤条件每行开头的缩进!')
    return df1, df2
# 后置过滤
def filter_after(df1, df2,exec_list):
    d = {'df1': df1, 'df2': df2}
    for content in exec_list:
        try:
            exec(content, globals(), d)
            df1 = d['df1']
            df2 = d['df2']
        except IndentationError as e:
            raise ValueError(f'{e}:', '请删掉过滤条件每行开头的缩进!')
    return df1, df2
def gen_selected(df, select_coin_num,sell_coin_num,buy_p,sell_p,header_columns,filter_before_exec,filter_after_exec):
    df1 = df.copy()
    df2 = df.copy()
    # 前置过滤
    df1, df2 = filter_before(df1, df2, filter_before_exec)

    # 根据因子对比进行排名
    # 从小到大排序
    df1['排名1'] = df1.groupby('candle_begin_time')['多头因子'].rank(method='first')
    df1 = df1[(df1['排名1'] <= select_coin_num)].copy()
    df1['方向'] = 1

    # 从大到小排序
    df2['排名2'] = df2.groupby('candle_begin_time')['空头因子'].rank(
        method='first', ascending=False)
    df2 = df2[(df2['排名2'] <= sell_coin_num)].copy()
    df2['方向'] = -1

    df1['排名'] = df1['排名1']
    df2['排名'] = df2['排名2']
    del df2['排名2']
    del df1['排名1']
    df1.sort_values('candle_begin_time', ascending=True, inplace=True)
    df1.reset_index(drop=True, inplace=True)
    df2.sort_values('candle_begin_time', ascending=True, inplace=True)
    df2.reset_index(drop=True, inplace=True)
    long_weight_array = w_log(p=buy_p, coins_num=select_coin_num)
    short_weight_array = w_log(p=sell_p, coins_num=sell_coin_num)
    for rank, w in enumerate(long_weight_array):
        df1.loc[df1['排名'] == rank + 1, 'weight'] = w
    for rank, w in enumerate(short_weight_array):
        df2.loc[df2['排名'] == rank + 1, 'weight'] = w
    # 后置过滤
    df1, df2 = filter_after(df1, df2, filter_after_exec)
    return df1[header_columns], df2[header_columns]

# 可视化
def playback_plot(curve):
    import matplotlib as mpl
    mpl.rcParams["font.sans-serif"] = ["SimHei"]  # 展示中文字体
    mpl.rcParams["axes.unicode_minus"] = False  # 处理负刻度值
    nv = curve.iloc[:, 0]
    dd = (nv / nv.cummax() - 1) * 100
    fig, ax1 = plt.subplots()  # subplots一定要带s
    fig.set_size_inches(14, 8)
    ax2 = ax1.twinx()  # twinx将ax1的X轴共用与ax2，这步很重要
    ax1.fill_between(dd.index, 0, dd, color='#95a3a6', alpha=0.4)
    # ax1.set_ylabel('Log')
    ax2.plot(nv, c='r')
    ax2.grid(True, axis='both', color='#95a3a6')
    ax1.grid(False, axis='y')
    ax1.tick_params(labelsize=18)
    ax2.tick_params(labelsize=18)
    ax1.legend(['最大回撤'], loc='center left', fontsize=18)
    ax2.legend(['净值'], loc='center right', fontsize=18)
    # ax2.set_ylabel('Log')
    ax2.set_yscale('log')
    ax2.set_title(f'中性策略回放', fontsize=24)
    plt.show()
def plot_output(x,data,data_path,save_html=True):
    x = x.copy()
    data.index.name = ''
    data = data[['累积净值','年化收益','月化收益', '月信息比', '月化波动', '月换手率', '月化收益回撤比', '累积净值', '最大回撤', '最大回撤开始时间',
       '最大回撤结束时间', '胜率', '盈亏收益比', '单周期最大盈利',
       '单周期大亏损','交易费率', 'leverage']].reset_index()
    data['交易费率'] = data['交易费率'].round(1).astype('int')
    part1 = data.iloc[:, :1].T.values.tolist()

    part2 = np.round(data.iloc[:, 1:9].T.values, 2).tolist()

    part3 = data.iloc[:, 9:].T.values.tolist()


    values = part1 + part2 + part3
    x['net_value'] = x['资金曲线'].round(4)

    x.reset_index(inplace=True)
    x['long_hold_symbol'] = x['long_hold_symbol'].str.replace('USDT','')
    x['short_hold_symbol'] = x['short_hold_symbol'].str.replace('USDT','')

    fig = make_subplots(
        rows=3, cols=1, shared_xaxes=True, vertical_spacing=0.02,
        specs=[[{"type": "table", "secondary_y": False}],
               [{"type": "xy", "secondary_y": True}],
               [{"type": "xy", "secondary_y": True}]],
        row_heights=[0.1, 0.75, 0.15],
    )

    # 主图
    fig.add_trace(
        go.Scatter(x=x['candle_begin_time'], y=x['net_value'], mode='lines', name='策略净值',
                   text=x['long_hold_symbol'] + '  ---  ' + x['short_hold_symbol']),
        secondary_y=False, row=2, col=1,
    )

    fig.add_trace(
        go.Scatter(x=x['candle_begin_time'], y=(x['net_value'] / x['net_value'].cummax() - 1).round(4), mode='lines', name='最大回撤',
                   line={'color': 'rgba(192,192,192,0.6)', 'width': 1}),
        secondary_y=True, row=2, col=1,
    )

    # 副图
    fig.add_trace(
        go.Scatter(x=x['candle_begin_time'], y=x['多头占比'], mode='none', name='多头杠杆率', stackgroup='one'),
        secondary_y=False, row=3, col=1,
    )
    fig.add_trace(
        go.Scatter(x=x['candle_begin_time'], y=x['空头占比'], mode='none', name='空头杠杆率', stackgroup='one'),
        secondary_y=False, row=3, col=1,
    )
    fig.add_trace(
        go.Bar(x=x['candle_begin_time'], y=(x['多头占比'] - x['空头占比']) , name='多空敞口差额'),
        secondary_y=False, row=3, col=1,
    )
    fig.add_trace(
        go.Table(
            header=dict(values=list(data.columns),  # 表头取值是data列属性
                        fill_color='paleturquoise',  # 填充色和文本位置
                        align='center'),
            cells=dict(values=values,  # 单元格的取值就是每个列属性的Series取值
                       fill_color='lavender',
                       align='center'
                       ),
            columnwidth=[90,40,40,35,35,35,35,50,35,35,90,90,30,40,60,60,40,40]),
        secondary_y=False, row=1, col=1,
    )
    fig.update_layout(
        yaxis_type='log', yaxis2_type='linear',
        template='none', hovermode='x', width=1650, height=950,
        xaxis_rangeslider_visible=False,
    )
    html_path = os.path.join(data_path, '净值曲线持仓图.html')

    if save_html:
        fig.write_html(file=html_path, config={'scrollZoom': True})
    else:
        fig.show(config={'scrollZoom': True})

def curve_playback(curve, play_start_time, step=6, sleeptime=0.2):
    import matplotlib as mpl
    # === 绘图显示中文
    if platform.system() == 'Windows':
        # windows
        plt.rcParams['font.sans-serif'] = ['SimHei']
        plt.rcParams['axes.unicode_minus'] = False
    else:
        # mac
        plt.rcParams['font.sans-serif'] = ['Arial Unicode MS']  # 指定默认字体
    nv = curve.iloc[:, 0]
    dd = (nv / nv.cummax() - 1) * 100
    fig, ax1 = plt.subplots()  # subplots一定要带s
    fig.set_size_inches(14, 8)
    ax2 = ax1.twinx()  # twinx将ax1的X轴共用与ax2，这步很重要
    _curve = curve.reset_index()
    i = _curve[_curve['candle_begin_time'] >= play_start_time].index[0]
    _i = i - 1
    for i in range(i, curve.shape[0], step):
        end_time = curve.index[i]
        ax1.cla()
        ax2.cla()
        #     ax1.plot(curve['最大回撤'].iloc[:i],c='r')
        ax1.fill_between(dd.iloc[:i].index, 0, dd.iloc[:i], color='#95a3a6', alpha=0.4)
        # ax1.set_ylabel('EXP')
        ax2.plot(nv.iloc[:_i], c='r')
        ax2.plot(nv.iloc[_i:i], c='blue')
        ax2.grid(True, axis='both', color='#95a3a6')
        ax1.grid(False, axis='y')
        ax1.tick_params(labelsize=18)
        ax2.tick_params(labelsize=18)
        ax1.legend(['最大回撤'], loc='center left', fontsize=18)
        ax2.legend(['净值'], loc='center right', fontsize=18)
        # ax2.set_ylabel('Log')
        ax2.set_title(f'策略回放 起始时间：{curve.index[_i]}   截止时间:{end_time}', fontsize=18)
        plt.pause(sleeptime)
    plt.show()

def plot(select_c, mdd_std=0.2):
    # plt.rcParams['axes.unicode_minus'] = False      # 用来正常显示负号
    # plt.figure(figsize=(12, 6), dpi=80)
    # plt.figure(1)

    condition = (select_c['dd2here'] >= -mdd_std) & (select_c['dd2here'].shift(1) < -mdd_std)
    select_c[f'回撤上穿{mdd_std}次数'] = 0
    select_c.loc[condition, f'回撤上穿{mdd_std}次数'] = 1
    mdd_num = int(select_c[f'回撤上穿{mdd_std}次数'].sum())
    ax = plt.subplot(2, 1, 1)

    plt.subplots_adjust(hspace=1)  # 调整子图间距
    plt.title(f'Back draw{mdd_std} Number: {mdd_num}', fontsize='large', fontweight = 'bold',color='blue', loc='center')  # 设置字体大小与格式
    ax.plot(select_c['candle_begin_time'], select_c['资金曲线'])
    ax2 = ax.twinx() # 设置y轴次轴
    ax2.plot(select_c["candle_begin_time"], -select_c['dd2here'], color='red', alpha=0.4)
# plt.show()
def plot_log(ret, title):
    # plt.rcParams['axes.unicode_minus'] = False      # 用来正常显示负号
    # plt.figure(figsize=(12, 6), dpi=80)
    # plt.figure(1)
    import matplotlib
    ax = plt.subplot(2, 1, 2)
    ax_left = ax
    ax_right = ax_left.twinx()
    ret = ret.copy()
    ret.index = pd.to_datetime(ret.index)
    nv = (1 + ret).cumprod()  # 净值
    dd = nv / nv.cummax() - 1  # 回撤
    # 右轴：净值曲线
    ax_right.grid(False)
    ax_right.plot(nv.index, nv.values, color='red')
    ax_right.set(xlim=(nv.index[0], nv.index[-1]))
    # 左轴：ax, 回撤
    # y2 = dd.values * 100
    y2 = dd['本周期多空涨跌幅'] * 100
    ax_left.fill_between(dd.index, 0, y2, color='#95a3a6', alpha=0.4)
    ax_left.set_ylim((ax_left.get_ylim()[0], 0))
    ax_left.yaxis.set_major_formatter(matplotlib.ticker.FormatStrFormatter('%.1f%%'))
    ax_left.grid(False, axis='y')
    # ax_right.grid(True, axis='y', color='#95a3a6')
    ax_right.grid(True, axis='both', color='#95a3a6')
    ax_left.legend(['回撤'], loc='center left')
    ax_right.legend(['净值'], loc='center right')
    if title is not None:
        ax_left.set_title(title)

    ax_right.set_yscale('log')
def multi_plot(curve):
    # 原版评价作图
    curve['本周期多空涨跌幅'] = curve['资金曲线'].pct_change().fillna(0)
    curve = curve.reset_index()
    rtn, select_c = ind.cal_ind(curve)
    # print(rtn.to_markdown())
    # print('\n')

    # === 绘图显示中文
    if platform.system() == 'Windows':
        # windows
        plt.rcParams['font.sans-serif'] = ['SimHei']
        plt.rcParams['axes.unicode_minus'] = False
    elif platform.system() == 'Linux':
        # Linux
        plt.rcParams['font.sans-serif'] = ['AR PL UKai CN']  # 指定默认字体
    else:
        # mac
        plt.rcParams['font.sans-serif'] = ['Arial Unicode MS']  # 指定默认字体
    plt.figure(figsize=(12, 6), dpi=80)
    plt.figure(1)

    # 收益回撤曲线图
    plot(select_c, mdd_std=0.2)
    # 对数图
    ret = curve[['candle_begin_time', '本周期多空涨跌幅']]
    ret = ret.set_index('candle_begin_time')
    title = '收益(对数坐标)-回撤'
    plot_log(ret, title)
    plt.show()
def plot_log_double(curve, mdd_std=0.2, path='./'):
    # 原版评价作图
    curve['本周期多空涨跌幅'] = curve['资金曲线'].pct_change().fillna(0)
    curve = curve.reset_index()
    all_select_df = curve
    rtn, select_c = ind.cal_ind(curve)
    # === 绘图显示中文
    if platform.system() == 'Windows':
        # windows
        plt.rcParams['font.sans-serif'] = ['SimHei']
        plt.rcParams['axes.unicode_minus'] = False
    elif platform.system() == 'Linux':
        # Linux
        plt.rcParams['font.sans-serif'] = ['AR PL UKai CN']  # 指定默认字体
    else:
        # mac
        plt.rcParams['font.sans-serif'] = ['Arial Unicode MS']  # 指定默认字体
    plt.figure(figsize=(12, 6), dpi=80)
    condition = (select_c['dd2here'] >= -mdd_std) & (select_c['dd2here'].shift(1) < -mdd_std)
    select_c[f'回撤上穿{mdd_std}次数'] = 0
    select_c.loc[condition, f'回撤上穿{mdd_std}次数'] = 1
    mdd_num = int(select_c[f'回撤上穿{mdd_std}次数'].sum())
    ax = plt.subplot(2, 1, 1)

    plt.subplots_adjust(hspace=1)  # 调整子图间距
    plt.title(f'Back draw{mdd_std} Number: {mdd_num}', fontsize='large', fontweight='bold', color='blue',
              loc='center')  # 设置字体大小与格式
    ax.plot(select_c['candle_begin_time'], select_c['资金曲线'])
    ax2 = ax.twinx()  # 设置y轴次轴
    ax2.plot(select_c["candle_begin_time"], -select_c['dd2here'], color='red', alpha=0.4)

    # 对数图
    ret = all_select_df[['candle_begin_time', '本周期多空涨跌幅']]
    ret = ret.set_index('candle_begin_time')
    title = 'Balance Curve(Log) - Back draw'
    ax = plt.subplot(2, 1, 2)
    ax_left = ax
    ax_right = ax_left.twinx()
    ret = ret.copy()
    ret.index = pd.to_datetime(ret.index)
    nv = (1 + ret).cumprod()  # 净值
    dd = nv / nv.cummax() - 1  # 回撤
    # 右轴：净值曲线
    ax_right.grid(False)
    ax_right.plot(nv.index, nv.values, color='red')
    ax_right.set(xlim=(nv.index[0], nv.index[-1]))
    # 左轴：ax, 回撤
    # y2 = dd.values * 100
    y2 = dd['本周期多空涨跌幅'] * 100
    ax_left.fill_between(dd.index, 0, y2, color='#95a3a6', alpha=0.4)
    ax_left.set_ylim((ax_left.get_ylim()[0], 0))
    ax_left.yaxis.set_major_formatter(tc.FormatStrFormatter('%.1f%%'))
    ax_left.grid(False, axis='y')
    ax_right.grid(True, axis='both', color='#95a3a6')
    ax_left.legend(['Back draw'], loc='center left')
    ax_right.legend(['Balance'], loc='center right')
    if title is not None:
        ax_left.set_title(title)

    # 倍增图叠加
    balance_list = [all_select_df.loc[0, '资金曲线']]
    time_list = [all_select_df.loc[0, 'candle_begin_time']]
    balance = all_select_df.loc[0, '资金曲线']
    while balance <= all_select_df['资金曲线'].max():
        balance *= 2
        _df = all_select_df[all_select_df['资金曲线'] >= balance]
        _df.reset_index(drop=True, inplace=True)
        if _df.shape[0] > 0:
            balance_list.append(_df.loc[0, '资金曲线'])
            time_list.append(_df.loc[0, 'candle_begin_time'])
    ax_right.scatter(time_list, balance_list, color='red')
    ax_right.set_yscale('log')

    plt.show()