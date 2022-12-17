#!/usr/bin/python3
# -*- coding: utf-8 -*-

from warnings import simplefilter
import warnings
from datetime import timedelta

import pandas as pd

from function import *
import re
from functools import partial

pd.set_option('expand_frame_repr', False)
pd.set_option('display.max_rows', 1000)  # 最多显示数据的行数
# setcopywarning
pd.set_option('mode.chained_assignment', None)
# UserWarning
# warnings.filterwarnings("ignore")
# FutureWarning
simplefilter(action='ignore', category=FutureWarning)
# ===常规配置
config_type = np.dtype(
    {
        'names': [
            'c_rate',
            'hold_hour_num',
            'select_coin_num',
            'sell_coin_num',
            'buy_p',
            'sell_p',
            'leverage',
            'long_risk_position',
            'initial_trade_usdt'],
        'formats': [
            np.float64,
            np.int64,
            np.int64,
            np.int64,
            np.float64,
            np.float64,
            np.float64,
            np.float64,
            np.float64,
       ]})
playCfg = np.zeros((1), dtype=config_type)

def playback_start(playCfg, othCfg):
    cal_factor_type, hourly_details, select_by_hour, filter_before_exec, filter_after_exec, start_date, end_date, factor_list1, factor_list2, \
    trade_type, compound_name, quit_symbol_filter_hour, p_signal_fun,select_offsets,white_list,black_list = load_othCfg(othCfg)
    t = time.time()
    hold_hour = str(playCfg['hold_hour_num'][0]) + 'H'
    # 提取前置过滤因子
    filter_list = []
    [filter_list.extend(re.findall(r"\['(.+?)'\]", x))
     for x in filter_before_exec + filter_after_exec]

    if 'fundingRate' in filter_list:
        use_fundingRate = True
        filter_list.remove('fundingRate')
    else:
        use_fundingRate = False
    filter_list = list(set(filter_list))
    start_date = pd.to_datetime(start_date)
    end_date = pd.to_datetime(end_date)
    all_factor_list = factor_list1 + factor_list2
    factor_class_list = tools.convert_to_cls(all_factor_list)
    filter_class_list = tools.convert_to_filter_cls(filter_list)
    feature_list = tools.convert_to_feature(all_factor_list)




    # ===读取数据
    df = reader.readhour(
        trade_type,
        factor_class_list,
        filter_class_list=filter_class_list)
    if df['candle_begin_time'].max() < pd.to_datetime(end_date):
        data_modify_time = df['candle_begin_time'].max() - timedelta(hours=1)
        print(f'[Warning]:本地数据最新日期小于设定回测结束时间，请检查。本次回测结束时间将被改为:{data_modify_time}')
        end_date = data_modify_time
    if df['candle_begin_time'].min() > pd.to_datetime(start_date) - timedelta(hours=int(playCfg['hold_hour_num'][0])):
        data_modify_time = df['candle_begin_time'].min() + timedelta(hours=int(playCfg['hold_hour_num'][0]))
        print(f'[Warning]:本地数据最早日期大于设定回测开始时间，请检查。本次回测开始时间将被改为:{data_modify_time}')
        start_date = data_modify_time


    # 消除因子计算时的shift，保持和实盘流程一致
    df[filter_list +
        feature_list] = df.groupby('symbol')[filter_list + feature_list].shift(-1)
    # 筛选日期范围
    df = df[df['candle_begin_time'] >= pd.to_datetime(
        start_date) - timedelta(hours=int(playCfg['hold_hour_num'][0]))]
    df = df[df['candle_begin_time'] <= pd.to_datetime(end_date)]

    all_symbol_list = sorted(list(set(df['symbol'].unique())))
    replace_symbol_to_int = {v: k for k, v in enumerate(all_symbol_list)}
    replace_symbol_to_int_ = {k: v for k, v in enumerate(all_symbol_list)}
    df['symbol'] = df['symbol'].replace(replace_symbol_to_int)
    symbols_data = df[['candle_begin_time', 'symbol', 'close', 'avg_price']]

    # 删除某些行数据
    df = df[df['volume'] > 0]  # 该周期不交易的币种
    # 最后几行数据，下个周期_avg_price为空
    df.dropna(subset=['下个周期_avg_price'], inplace=True)
    # ===数据预处理
    df = df[['candle_begin_time', 'close', 'symbol'] +
            feature_list + filter_list]
    df = df.set_index(['candle_begin_time', 'symbol']).sort_index()
    df = df.replace([np.inf, -np.inf], np.nan)
    # 因子空值都用中位数填充, 如果填0可能后面rank排序在第一或者最后
    #df = df.fillna(value=0)
    df[feature_list] = df[feature_list].apply(lambda x: x.fillna(x.median()))
    df = df.reset_index()

    if use_fundingRate:
        # ===整合资金费率
        fundingrate_data = reader.read_fundingrate()
        fundingrate_data['symbol'] = fundingrate_data['symbol'].replace(replace_symbol_to_int)
        df = pd.merge(df,
                      fundingrate_data[['candle_begin_time', 'symbol', 'fundingRate']],
                      on=['candle_begin_time', 'symbol'], how="left")
        df['fundingRate'].fillna(value=0, inplace=True)
        print('整合资金费率完毕!!!\n')

    # 提前排除退市币种
    # 提前排除退市币种
    df2 = df.reset_index()
    max_time = df['candle_begin_time'].max()
    quit_df = df.groupby('symbol')['candle_begin_time'].max().to_frame()
    quit_df = quit_df[quit_df['candle_begin_time'] < max_time]
    quit_symbols = quit_df.index.tolist()
    quit_df_ = df[df['symbol'].isin(quit_symbols)]
    noquit_df = df[~df['symbol'].isin(quit_symbols)]
    # 退市币种的处理,实盘提前N小时加入黑名单
    quit_df_ = quit_df_.groupby('symbol', group_keys=False).apply(
        lambda x: x.iloc[:-quit_symbol_filter_hour - 1])
    df = noquit_df.append(quit_df_)
    df_quit = quit_df_.groupby('symbol').tail(1)
    if df_quit.empty:
        quit_arry = np.array([])
    else:
        df_quit['runtime'] = ((df_quit['candle_begin_time'] - pd.to_datetime(start_date)).dt.total_seconds()/3600).astype('int')
        quit_arry = df_quit[['runtime','symbol']].values


    print('数据处理完毕!!!\n')
    print('数据预处理耗时', time.time() - t)

    t = time.time()
    # ===计算因子
    if cal_factor_type == 'cross':
        # 横截面
        df = cal_factor_by_cross(df, factor_list1, factor_list2)
    elif cal_factor_type == 'verical':
        # 纵截面
        df = cal_factor_by_verical(df, factor_list1, factor_list2)
    else:
        raise ValueError('cal_factor_type set error!')
    print('因子计算耗时', time.time() - t)


    t = time.time()
    # numpy 选币
    base_index = pd.date_range(start=start_date -timedelta(hours=int(playCfg['hold_hour_num'][0])),end=end_date,freq='1H').tolist()
    select_coin_long, select_coin_short, arr_data = np_gen_selected(
        df, base_index,filter_before_exec, filter_after_exec, select_by_hour, playCfg, select_offsets,white_list,black_list,replace_symbol_to_int)
    print('选币耗时', time.time() - t, '\n')

    t = time.time()
    res, curve, account_df, display_df, order_df = neutral_strategy_playback(
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
        select_coin_long,
        select_coin_short,
        compound_name=compound_name,
        hourly_details=hourly_details)
    data_path = os.path.join(root_path,'data','中性回放结果', compound_name)
    if not os.path.exists(data_path):
        os.mkdir(data_path)
    # 回放数据保存
    save_path = os.path.join(data_path, '净值持仓数据.csv')
    res.to_csv(save_path, encoding='gbk')
    curve.to_csv(save_path, encoding='gbk',mode='a')
    save_path = os.path.join(data_path, '虚拟账户数据.csv')
    account_df.to_csv(save_path, encoding='gbk')
    save_path = os.path.join(data_path, '持仓面板数据.pkl')
    display_df.to_pickle(save_path)
    save_path = os.path.join(data_path, '下单面板数据.pkl')
    order_df.to_pickle(save_path)

    print('回放耗时：', time.time() - t, '\n')

    # print(res)

    print(res.to_markdown())
    print('\n')
    # print(account_df)

    # plotly 作图
    plot_output(curve,res,data_path, save_html=True)
    # 船队作图整合
    plot_log_double(curve)
    return res


def data_processing(all_factor_calss_list,all_filter_class_list,playCfg, othCfg)-> pd.DataFrame: 

    cal_factor_type, hourly_details, select_by_hour, filter_before_exec, filter_after_exec, start_date, end_date, factor_list1, factor_list2, \
    trade_type, compound_name, quit_symbol_filter_hour, p_signal_fun,select_offsets,white_list,black_list = load_othCfg(othCfg)
    t = time.time()
    hold_hour = str(playCfg['hold_hour_num'][0]) + 'H'


    if 'fundingRate' in all_filter_class_list:
        use_fundingRate = True
        all_filter_class_list.remove('fundingRate')
    else:
        use_fundingRate = False
    all_filter_class_list = list(set(all_filter_class_list))
    start_date = pd.to_datetime(start_date)
    end_date = pd.to_datetime(end_date)


    # ===读取数据 生成因子数据
    """
    factor_class_list = [Factor1, Factor2, ...] adaptbollingv3
    filter_class_list = [Filter1, Filter2, ...] 涨跌幅max
    """
    df = reader.readhour(
        trade_type,
        all_factor_calss_list,
        filter_class_list=all_filter_class_list)
    if df['candle_begin_time'].max() < pd.to_datetime(end_date):
        data_modify_time = df['candle_begin_time'].max() - timedelta(hours=1)
        print(f'[Warning]:本地数据最新日期小于设定回测结束时间，请检查。本次回测结束时间将被改为:{data_modify_time}')
        end_date = data_modify_time
    if df['candle_begin_time'].min() > pd.to_datetime(start_date) - timedelta(hours=int(playCfg['hold_hour_num'][0])):
        data_modify_time = df['candle_begin_time'].min() + timedelta(hours=int(playCfg['hold_hour_num'][0]))
        print(f'[Warning]:本地数据最早日期大于设定回测开始时间，请检查。本次回测开始时间将被改为:{data_modify_time}')
        start_date = data_modify_time


    #读取所有数据还有因子数据以后,将数据中的选币因子数据和过滤因子数据分开,分成两个列表
    df_columns = df.columns.tolist()
    #df_columns里包含all_factor_calss_list字符串的列名
    feature_list = [x for x in df_columns if any([y in x for y in all_factor_calss_list])]
    filter_list = [x for x in df_columns if any([y in x for y in all_filter_class_list])]

    # # 消除因子计算时的shift，保持和实盘流程一致
    # df[filter_list +
    #     feature_list] = df.groupby('symbol')[filter_list + feature_list].shift(-1)
    # # 筛选日期范围
    # df = df[df['candle_begin_time'] >= pd.to_datetime(
    #     start_date) - timedelta(hours=int(playCfg['hold_hour_num'][0]))]
    # df = df[df['candle_begin_time'] <= pd.to_datetime(end_date)]

    # all_symbol_list = sorted(list(set(df['symbol'].unique())))
    # replace_symbol_to_int = {v: k for k, v in enumerate(all_symbol_list)}
    # replace_symbol_to_int_ = {k: v for k, v in enumerate(all_symbol_list)}
    # df['symbol'] = df['symbol'].replace(replace_symbol_to_int)
    # symbols_data = df[['candle_begin_time', 'symbol', 'close', 'avg_price']]

    # # 删除某些行数据
    # df = df[df['volume'] > 0]  # 该周期不交易的币种
    # # 最后几行数据，下个周期_avg_price为空
    # df.dropna(subset=['下个周期_avg_price'], inplace=True)
    # # ===数据预处理
    # df = df[['candle_begin_time', 'close', 'symbol'] +
    #         feature_list + filter_list]
    # df = df.set_index(['candle_begin_time', 'symbol']).sort_index()
    # df = df.replace([np.inf, -np.inf], np.nan)
    # # 因子空值都用中位数填充, 如果填0可能后面rank排序在第一或者最后
    # #df = df.fillna(value=0)
    # df[feature_list] = df[feature_list].apply(lambda x: x.fillna(x.median()))
    # df = df.reset_index()

    # if use_fundingRate:
    #     # ===整合资金费率
    #     fundingrate_data = reader.read_fundingrate()
    #     fundingrate_data['symbol'] = fundingrate_data['symbol'].replace(replace_symbol_to_int)
    #     df = pd.merge(df,
    #                   fundingrate_data[['candle_begin_time', 'symbol', 'fundingRate']],
    #                   on=['candle_begin_time', 'symbol'], how="left")
    #     df['fundingRate'].fillna(value=0, inplace=True)
    #     print('整合资金费率完毕!!!\n')

    # # 提前排除退市币种
    # # 提前排除退市币种
    # df2 = df.reset_index()
    # max_time = df['candle_begin_time'].max()
    # quit_df = df.groupby('symbol')['candle_begin_time'].max().to_frame()
    # quit_df = quit_df[quit_df['candle_begin_time'] < max_time]
    # quit_symbols = quit_df.index.tolist()
    # quit_df_ = df[df['symbol'].isin(quit_symbols)]
    # noquit_df = df[~df['symbol'].isin(quit_symbols)]
    # # 退市币种的处理,实盘提前N小时加入黑名单
    # quit_df_ = quit_df_.groupby('symbol', group_keys=False).apply(
    #     lambda x: x.iloc[:-quit_symbol_filter_hour - 1])
    # df = noquit_df.append(quit_df_)
    # df_quit = quit_df_.groupby('symbol').tail(1)
    # if df_quit.empty:
    #     quit_arry = np.array([])
    # else:
    #     df_quit['runtime'] = ((df_quit['candle_begin_time'] - pd.to_datetime(start_date)).dt.total_seconds()/3600).astype('int')
    #     quit_arry = df_quit[['runtime','symbol']].values


    print('数据处理完毕!!!\n')
    print('数据预处理耗时', time.time() - t)
    return df,feature_list,filter_list

#回测单因子+单过滤器
def backtest_single_factor_single_filter(_df,playCfg,othCfg):
    df = _df.copy()

    cal_factor_type, hourly_details, select_by_hour, filter_before_exec, filter_after_exec, start_date, end_date, factor_list1, factor_list2, \
    trade_type, compound_name, quit_symbol_filter_hour, p_signal_fun,select_offsets,white_list,black_list = load_othCfg(othCfg)
    t = time.time()
    hold_hour = str(playCfg['hold_hour_num'][0]) + 'H'
    # 提取前置过滤因子
    filter_list = []
    [filter_list.extend(re.findall(r"\['(.+?)'\]", x))
     for x in filter_before_exec + filter_after_exec]

    if 'fundingRate' in filter_list:
        use_fundingRate = True
        filter_list.remove('fundingRate')
    else:
        use_fundingRate = False
    filter_list = list(set(filter_list))
    start_date = pd.to_datetime(start_date)
    end_date = pd.to_datetime(end_date)
    all_factor_list = factor_list1 + factor_list2
    factor_class_list = tools.convert_to_cls(all_factor_list)
    filter_class_list = tools.convert_to_filter_cls(filter_list)
    feature_list = tools.convert_to_feature(all_factor_list)

    # # ===读取数据
    # df = reader.readhour(
    #     trade_type,
    #     factor_class_list,
    #     filter_class_list=filter_class_list)
    # if df['candle_begin_time'].max() < pd.to_datetime(end_date):
    #     data_modify_time = df['candle_begin_time'].max() - timedelta(hours=1)
    #     print(f'[Warning]:本地数据最新日期小于设定回测结束时间，请检查。本次回测结束时间将被改为:{data_modify_time}')
    #     end_date = data_modify_time
    # if df['candle_begin_time'].min() > pd.to_datetime(start_date) - timedelta(hours=int(playCfg['hold_hour_num'][0])):
    #     data_modify_time = df['candle_begin_time'].min() + timedelta(hours=int(playCfg['hold_hour_num'][0]))
    #     print(f'[Warning]:本地数据最早日期大于设定回测开始时间，请检查。本次回测开始时间将被改为:{data_modify_time}')
    #     start_date = data_modify_time


    # 消除因子计算时的shift，保持和实盘流程一致

    df[filter_list +
        feature_list] = df.groupby('symbol')[filter_list + feature_list].shift(-1)
    # 筛选日期范围
    df = df[df['candle_begin_time'] >= pd.to_datetime(
        start_date) - timedelta(hours=int(playCfg['hold_hour_num'][0]))]
    df = df[df['candle_begin_time'] <= pd.to_datetime(end_date)]

    all_symbol_list = sorted(list(set(df['symbol'].unique())))
    replace_symbol_to_int = {v: k for k, v in enumerate(all_symbol_list)}
    replace_symbol_to_int_ = {k: v for k, v in enumerate(all_symbol_list)}
    df['symbol'] = df['symbol'].replace(replace_symbol_to_int)
    symbols_data = df[['candle_begin_time', 'symbol', 'close', 'avg_price']]

    # 删除某些行数据
    df = df[df['volume'] > 0]  # 该周期不交易的币种
    # 最后几行数据，下个周期_avg_price为空
    df.dropna(subset=['下个周期_avg_price'], inplace=True)
    # ===数据预处理
    df = df[['candle_begin_time', 'close', 'symbol'] +
            feature_list + filter_list]
    df = df.set_index(['candle_begin_time', 'symbol']).sort_index()
    df = df.replace([np.inf, -np.inf], np.nan)
    # 因子空值都用中位数填充, 如果填0可能后面rank排序在第一或者最后
    #df = df.fillna(value=0)
    df[feature_list] = df[feature_list].apply(lambda x: x.fillna(x.median()))
    df = df.reset_index()

    if use_fundingRate:
        # ===整合资金费率
        fundingrate_data = reader.read_fundingrate()
        fundingrate_data['symbol'] = fundingrate_data['symbol'].replace(replace_symbol_to_int)
        df = pd.merge(df,
                      fundingrate_data[['candle_begin_time', 'symbol', 'fundingRate']],
                      on=['candle_begin_time', 'symbol'], how="left")
        df['fundingRate'].fillna(value=0, inplace=True)
        print('整合资金费率完毕!!!\n')

 
    # 提前排除退市币种
    df2 = df.reset_index()
    max_time = df['candle_begin_time'].max()
    quit_df = df.groupby('symbol')['candle_begin_time'].max().to_frame()
    quit_df = quit_df[quit_df['candle_begin_time'] < max_time]
    quit_symbols = quit_df.index.tolist()
    quit_df_ = df[df['symbol'].isin(quit_symbols)]
    noquit_df = df[~df['symbol'].isin(quit_symbols)]
    # 退市币种的处理,实盘提前N小时加入黑名单
    quit_df_ = quit_df_.groupby('symbol', group_keys=False).apply(
        lambda x: x.iloc[:-quit_symbol_filter_hour - 1])
    df = noquit_df.append(quit_df_)
    df_quit = quit_df_.groupby('symbol').tail(1)
    if df_quit.empty:
        quit_arry = np.array([])
    else:
        df_quit['runtime'] = ((df_quit['candle_begin_time'] - pd.to_datetime(start_date)).dt.total_seconds()/3600).astype('int')
        quit_arry = df_quit[['runtime','symbol']].values


    # print('数据处理完毕!!!\n')
    # print('数据预处理耗时', time.time() - t)

    t = time.time()
    # ===计算因子
    if cal_factor_type == 'cross':
        # 横截面
        df = cal_factor_by_cross(df, factor_list1, factor_list2)
    elif cal_factor_type == 'verical':
        # 纵截面
        df = cal_factor_by_verical(df, factor_list1, factor_list2)
    else:
        raise ValueError('cal_factor_type set error!')
    print('因子计算耗时', time.time() - t)


    t = time.time()
    # numpy 选币
    base_index = pd.date_range(start=start_date -timedelta(hours=int(playCfg['hold_hour_num'][0])),end=end_date,freq='1H').tolist()
    select_coin_long, select_coin_short, arr_data = np_gen_selected(
        df, base_index,filter_before_exec, filter_after_exec, select_by_hour, playCfg, select_offsets,white_list,black_list,replace_symbol_to_int)
    print('选币耗时', time.time() - t, '\n')

    t = time.time()
    res, curve, account_df, display_df, order_df = neutral_strategy_playback(
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
        select_coin_long,
        select_coin_short,
        compound_name=compound_name,
        hourly_details=hourly_details)
    data_path = os.path.join(root_path,'data','中性回放结果', compound_name)
    if not os.path.exists(data_path):
        os.mkdir(data_path)
    # 回放数据保存
    # save_path = os.path.join(data_path, '净值持仓数据.csv')
    # res.to_csv(save_path, encoding='gbk')
    # curve.to_csv(save_path, encoding='gbk',mode='a')
    # save_path = os.path.join(data_path, '虚拟账户数据.csv')
    # account_df.to_csv(save_path, encoding='gbk')
    # save_path = os.path.join(data_path, '持仓面板数据.pkl')
    # display_df.to_pickle(save_path)
    # save_path = os.path.join(data_path, '下单面板数据.pkl')
    # order_df.to_pickle(save_path)

    print('回放耗时：', time.time() - t, '\n')

    # print(res)

    print(res.to_markdown())
    print('\n')
    # print(account_df)

    # # plotly 作图
    # plot_output(curve,res,data_path, save_html=True)
    # # 船队作图整合
    # plot_log_double(curve)


    #判断factor_class_list和filter_class_listcsv文件是否存在
    if not os.path.exists(os.path.join('f1_1+1_output', '选币因子分类',f'{factor_class_list}.csv')):
        res.to_csv(os.path.join('f1_1+1_output', '选币因子分类',f'{factor_class_list}.csv'))
    else:
        res.to_csv(os.path.join('f1_1+1_output', '选币因子分类',f'{factor_class_list}.csv'),header =False,mode='a')

    if filter_class_list != [] and not os.path.exists(os.path.join('f1_1+1_output', '过滤因子分类',f'{filter_class_list}.csv')):
        res.to_csv(os.path.join('f1_1+1_output', '过滤因子分类',f'{filter_class_list}.csv'))
    elif filter_class_list != []:
        res.to_csv(os.path.join('f1_1+1_output', '过滤因子分类',f'{filter_class_list}.csv'),header =False,mode='a')
        

    
    del df
    return res