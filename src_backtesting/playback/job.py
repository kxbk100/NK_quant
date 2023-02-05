import re
from datetime import timedelta
from warnings import simplefilter

# from utils import reader

from function import *

pd.set_option('expand_frame_repr', False)
pd.set_option('display.max_rows', 1000)  # 最多显示数据的行数
# setcopywarning
pd.set_option('mode.chained_assignment', None)
# UserWarning
# warnings.filterwarnings("ignore")
# FutureWarning
simplefilter(action='ignore', category=FutureWarning)



def playback_start(playCfg, othCfg):
    log_level, cal_factor_type, hourly_details, select_by_hour, filter_before_exec, filter_after_exec, start_date, end_date, factor_long_list, factor_short_list, trade_type, compound_name, quit_symbol_filter_hour, p_signal_fun, select_offsets, white_list, black_list = load_othCfg(othCfg)
    log.remove()
    log.add(sys.stdout, level=log_level)
    hold_hour = str(playCfg['hold_hour_num'][0]) + 'H'
    # 提取前置过滤因子
    filter_list = []
    [filter_list.extend(re.findall(r"\['(.+?)'\]", x))
     for x in filter_before_exec + filter_after_exec]
    filter_list = list(set(filter_list))

    if 'fundingRate' in filter_list:
        use_fundingRate = True
        filter_list.remove('fundingRate')
    else:
        use_fundingRate = False
    start_date = pd.to_datetime(start_date)
    end_date = pd.to_datetime(end_date)

    all_factor_list = factor_long_list + factor_short_list
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
        log.warning(f'本地数据最新日期小于设定回测结束时间,请检查。本次回测结束时间将被改为:{data_modify_time}')
        end_date = data_modify_time
    if df['candle_begin_time'].min() > pd.to_datetime(start_date) - timedelta(hours=int(playCfg['hold_hour_num'][0])):
        data_modify_time = df['candle_begin_time'].min() + timedelta(hours=int(playCfg['hold_hour_num'][0]))
        log.warning(f'本地数据最早日期大于设定回测开始时间,请检查。本次回测开始时间将被改为:{data_modify_time}')
        start_date = data_modify_time


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
    # 最后几行数据,下个周期_avg_price为空
    df.dropna(subset=['下个周期_avg_price'], inplace=True)
    # ===数据预处理
    df = df[['candle_begin_time', 'close', 'symbol'] +
            feature_list + filter_list]
    df = df.set_index(['candle_begin_time', 'symbol']).sort_index()
    df = df.replace([np.inf, -np.inf], np.nan)
    # 因子空值都用中位数填充, 如果填0可能后面rank排序在第一或者最后
    # df = df.fillna(value=0)
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
        log.info('整合资金费率完成')

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
    if quit_df_.empty:
        quit_arry = np.array([])
    else:
        df_quit = quit_df_.groupby('symbol').tail(1)
        df_quit['runtime'] = ((df_quit['candle_begin_time'] - pd.to_datetime(start_date)).dt.total_seconds() / 3600).astype('int')
        quit_arry = df_quit[['runtime', 'symbol']].values
    log.info('数据处理完成')

    # ===计算因子
    if cal_factor_type == 'cross':
        # 横截面
        df = cal_factor_by_cross(df, factor_long_list, factor_short_list)
    elif cal_factor_type == 'vertical':
        # 纵截面
        df = cal_factor_by_vertical(df, factor_long_list, factor_short_list)
    else:
        raise ValueError('cal_factor_type set error!')
    log.info('因子计算完成')

    # numpy 选币
    base_index = pd.date_range(start=start_date - timedelta(hours=int(playCfg['hold_hour_num'][0])), end=end_date, freq='1H').tolist()
    select_coin_long, select_coin_short, arr_data = np_gen_selected(
        df, base_index, filter_before_exec, filter_after_exec, select_by_hour, playCfg, select_offsets, white_list, black_list, replace_symbol_to_int)
    log.info('选币完成')

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
    data_path = os.path.join(rtn_data_path, compound_name)
    if not os.path.exists(data_path):
        os.makedirs(data_path)
    # 回放数据保存
    save_path = os.path.join(data_path, '净值持仓数据.csv')
    res.to_csv(save_path, encoding='gbk')
    curve.to_csv(save_path, encoding='gbk', mode='a')
    save_path = os.path.join(data_path, '虚拟账户数据.csv')
    account_df.to_csv(save_path, encoding='gbk')
    save_path = os.path.join(data_path, '持仓面板数据.pkl')
    display_df.to_pickle(save_path)
    save_path = os.path.join(data_path, '下单面板数据.pkl')
    order_df.to_pickle(save_path)
    log.info(f'\n{res.to_markdown()}')

    # plotly 作图
    plot_output(curve, res, data_path, save_html=True)
    # 船队作图整合
    plot_log_double(curve)
    return res,curve
