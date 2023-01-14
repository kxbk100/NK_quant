import pprint
from itertools import product
from function import filter_generate, log, parallel_filter_handle
from job import playback_start
from environ import playCfg, RankAscending, FilterAfter, tag
import os
from function import *
import re
from datetime import timedelta


output_path = os.path.join(root_path,'data/参数遍历结果')
output_fa_path = os.path.join(output_path, '选币因子分类')
output_fi_path = os.path.join(output_path, '过滤因子分类')
if not os.path.exists(output_path):
	os.mkdir(output_path)
if not os.path.exists(output_fa_path):
	os.mkdir(output_fa_path)
if not os.path.exists(output_fi_path):
	os.mkdir(output_fi_path)



def  factor_config_transform_class(factor_config):
    factor_class_list = []
    for _factor in factor_config:

        if 'bh' in _factor:
            factor_class_list.append(_factor.split('_bh_')[0])
        else:
            factor_class_list.append(_factor)
    
    return factor_class_list

def filter_config_transform_class(filter_config):
    filter_class_list = []
    for _filter in filter_config:
        if 'fl' in _filter:
            filter_class_list.append(_filter.split('_fl_')[0])
        elif 'None' in _filter:
            filter_class_list=[]
        else:
            filter_class_list.append(_filter)
    return filter_class_list



def factor_list_transform_std_factor(feature_list):
    factor_list = []

    for feature in feature_list:
        factor =[]
        if 'bh' in feature:
            if '__diff__' in feature:
                
                factor_list.append([(feature.split('_bh_')[0],True,feature.split('_bh_')[-1].split('__diff__')[0],feature.split('_bh_')[-1].split('__diff__')[-1],1)])
                factor_list.append([(feature.split('_bh_')[0],False,feature.split('_bh_')[-1].split('__diff__')[0],feature.split('_bh_')[-1].split('__diff__')[-1],1)])
            elif '__diff__'not in feature:
                factor_list.append([(feature.split('_bh_')[0],True,feature.split('_bh_')[-1],0,1)])
                factor_list.append([(feature.split('_bh_')[0],False,feature.split('_bh_')[-1],0,1)])
    return factor_list
   
    #('AdaptBollingv3', True, 96, 0, 1)
def get_filter_params(filter_class,filter_before_list_params):
    res_list = []
    for  filter_before_param in filter_before_list_params:
        if filter_before_param[0] in filter_class:
            res_list.append(['df1',filter_class,filter_before_param[1],filter_before_param[2],filter_before_param[3],filter_before_param[4],filter_before_param[5]])
            res_list.append(['df2',filter_class,filter_before_param[1],filter_before_param[2],filter_before_param[3],filter_before_param[4],filter_before_param[5]])
    return res_list


#处理过滤
def filter_list_transform_std_filter(filter_list):
    filter_before_list = []
    filter_type_dict = {}

    
    for _filter in filter_list:
        #为空暂未处理
        if _filter == 'None':
            
            filter_before_list.append(['None'])
            return filter_before_list
            
        else:
            if _filter.split('_fl_')[0] not in filter_type_dict.keys():
                filter_type_dict[_filter.split('_fl_')[0]] = []
            filter_before_params = get_filter_params(_filter,filter_before_list_params)
            filter_before_exec = [filter_generate(param=param) for param in filter_before_params]
            #filter_type_dict 这个字典里的列表添加过滤
            filter_type_dict[_filter.split('_fl_')[0]].append(filter_before_exec)

    filter_before_list = list(product(*filter_type_dict.values()))
    #由于不同的过滤之前没有保存在同一个列表，所以需要将其合并
    res_list = []
    for filter_before in filter_before_list:
        _rse=[]
        for _filter_before in filter_before:
            for __filter_before in _filter_before:
                _rse.append(__filter_before)
        res_list.append(_rse)


    return res_list
            

                
            


# 运行回放
def run_play(_df,playCfg,othCfg):
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

    # # ===读取数据
    # df = reader.readhour(
    #     trade_type,
    #     factor_class_list,
    #     filter_class_list=filter_class_list)
    df = _df.copy()
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

    # # plotly 作图
    # plot_output(curve, res, data_path, save_html=True)
    # # 船队作图整合
    # plot_log_double(curve)

    #将因子参数添加到res中， 保存格式
    res
    res['因子名']=factor_long_list[0][0]
    res['因子TF']=factor_long_list[0][1]
    res['因子参数']=factor_long_list[0][2]
    res['因子差分']=factor_long_list[0][3]

    for i in range(len(filter_list)):
        res[f'过滤因子_{i+1}'] = filter_list[i].split('_fl_')[0]
        res[f'过滤因子_参数_{i+1}']=filter_list[i].split('_fl_')[1]

    if not os.path.exists(os.path.join(output_fa_path,f'{factor_class_list}.csv')):
        res.to_csv(os.path.join(output_fa_path,f'{factor_class_list}.csv'))
    else:        res.to_csv(os.path.join(output_fa_path,f'{factor_class_list}.csv'),header =False,mode='a')

    if filter_class_list == []:filter_class_list.append(None)
    if not os.path.exists(os.path.join(output_fi_path,f'{filter_class_list}.csv')):
        res.to_csv(os.path.join(output_fi_path,f'{filter_class_list}.csv'))
    else:
        res.to_csv(os.path.join(output_fi_path,f'{filter_class_list}.csv'),header =False,mode='a')




    return res






#==========================================================================================================

compound_name = 'AdaptBollingv3'  # name
# ===常规配置
cal_factor_type = 'cross'  # cross/ vertical
start_date = '2020-01-01'
end_date = '2022-11-20'
# factor_long_list = [('AdaptBollingv3', True, 96, 0, 1)]
# factor_short_list = [('AdaptBollingv3', True, 96, 0, 1)]


# 遍历写法规定 如果只写因子名,默认获取所有文件过去因子参数,支持多过滤条件
#需要修改funtion.PY  #if num > 9: raise ValueError('当前过滤范式不允许单向过滤个数超过9个')
# 1. 选币因子
# 选币因子不指定参数:'AdaptBollingv3'
# 选币因子指定参数:’AdaptBollingv3_bh_96’
# 2. 过滤因子
# 无过滤写法:'None' 
# 只写过滤名写法: '过滤因子名字'
# 指定过滤参数写法: '过滤因子名字_fl_参数'

#遍历流程
#1.根据选币因子和过滤因子的因子名,获取所有的读取数据列表的组合
#2.根据一组因子组合,获得所有的带参数的子配置,如果因子带参数,直接使用参数组合,如果因子只有名字,通过读取文件,获得所有的参数
#3.将所有的因子参数做笛卡尔积,获得所有的参数组合
#4.根据参数组合,获得所有的子配置,并运行回放


# ===遍历参数配置
ergodic_factor_list = [
    ['AdaptBollingv3_bh_96'],
    ['AdaptBollingv3'],
    ]
ergodic_filter_list = [
    ['None'],
    ['ZH_成交额_fl_24'],
    ['ZH_成交额','涨跌幅max'],

]

filter_before_list_params = [ 
    ['ZH_成交额', 'rank', 'lte', 30, RankAscending.FALSE, FilterAfter.FALSE],
    ['涨跌幅max', 'value', 'lte', 0.2, RankAscending.FALSE, FilterAfter.FALSE]
]

    


trade_type = 'swap'
playCfg['c_rate'] = 6 / 10000  # 手续费
playCfg['hold_hour_num'] = 12  # hold_hour
# 只跑指定的N个offset,空列表则全offset
long_select_offset = []
short_select_offset = []

# ===回放增强配置
playCfg['long_coin_num'] = 1  # 多头选币数
playCfg['short_coin_num'] = 1  # 空头选币数
# 多币权重参数(不小于0),多币时有效,详见https://bbs.quantclass.cn/thread/8835
playCfg['long_p'] = 0  # 0 :等权, 0 -> ∞ :多币头部集中度逐渐降低
playCfg['short_p'] = 0  # long_coin_num = 3, long_p = 1 ;rank 1,2,3 的资金分配 [0.43620858, 0.34568712, 0.21810429]
playCfg['leverage'] = 1  # 杠杆率
playCfg['long_risk_position'] = 0  # 多头风险暴露 0.1 对冲后 10% 净多头, -0.2 对冲后 20% 净空头
playCfg['initial_trade_usdt'] = 10000  # 初始投入,金额过小会导致某些币无法开仓
# offset 止盈止损,都为0时,该功能关闭
playCfg['offset_stop_win'] = 0  # offset 止盈
playCfg['offset_stop_loss'] = 0   # offset 止损

# 固定白名单 'BTCUSDT'
long_white_list = []
short_white_list = []
# 固定黑名单
long_black_list = []
short_black_list = []

# ===过滤配置(元素皆为字符串,仿照e.g.写即可 支持 & |)
# 前置过滤 筛选出选币的币池

# 写法1

filter_before_params = [

    # ['df1', 'Volume_fl_24', 'rank', 'lte', 30, RankAscending.FALSE, FilterAfter.FALSE],
    # ['df2', 'Volume_fl_24', 'rank', 'lte', 30, RankAscending.FALSE, FilterAfter.FALSE],
    ['df1', '涨跌幅max_fl_24', 'value', 'lte', 0.2, RankAscending.FALSE, FilterAfter.FALSE],
    ['df2', '涨跌幅max_fl_24', 'value', 'lte', 0.4, RankAscending.FALSE, FilterAfter.FALSE],

    # (
    #     ['df1', '费率max_fl_24', 'rank', 'gte', 5, RankAscending.FALSE, FilterAfter.FALSE],
    #     ['df1', 'fundingRate', 'value', 'lte', 0.0001, RankAscending.FALSE, FilterAfter.FALSE],
    #     '1|2'
    # ),
    # (
    #     ['df2', '费率min_fl_24', 'rank', 'gte', 5, RankAscending.TRUE, FilterAfter.FALSE],
    #     ['df2', 'fundingRate', 'value', 'gte', 0, RankAscending.FALSE, FilterAfter.FALSE],
    #     '1|2'
    # ),

    # (
    #     ['df1', '费率max_fl_24', 'pct', 'lte', 0.95, RankAscending.FALSE, FilterAfter.FALSE],
    #     ['df1', 'fundingRate', 'value', 'lte', 0.0001, RankAscending.FALSE, FilterAfter.FALSE],
    #     '1|2'
    # ),
    # (
    #     ['df2', '费率min_fl_24', 'pct', 'gte', 0.05, RankAscending.FALSE, FilterAfter.FALSE],
    #     ['df2', 'fundingRate', 'value', 'gte', 0, RankAscending.FALSE, FilterAfter.FALSE],
    #     '1|2'
    # )
]

filter_before_exec = [filter_generate(param=param) for param in filter_before_params]
# 将默认的串联过滤转化为并联,只针对前置过滤且有使用了rank/pct类型的过滤集,value类型串并联无影响
# filter_before_exec, tag = parallel_filter_handle(filter_before_exec)


# filter_info = """filter_factor = ['涨跌幅max_fl_24'][0]
# df1 = df1[df1[f'涨跌幅max_fl_24']<0.2]
# filter_factor = ['涨跌幅max_fl_24'][0]
# df2 = df2[df2[f'涨跌幅max_fl_24']<0.2]
# """
# filter_before_exec = [filter_info]


# 后置过滤 在选币后下单前控制选定币种的资金分配系数
filter_after_params = [
    # ['df2', 'fundingRate', 'value', 'lte', -0.0001, RankAscending.FALSE, FilterAfter.TRUE]
]
filter_after_exec = [filter_generate(param=param) for param in filter_after_params]

# ===花式配置
# 资金曲线择时:(param[-1] 默认为计算signal需要的最少小时数)
p_signal_fun = None
# param = [48, 48]
# p_signal_fun = partial(ma_signal, param)

# ===回放参数配置
hourly_details = False  # True 会生成详细截面数据 持仓面板和下单面板,耗时增加20S左右
select_by_hour = False  # True 为逐小时,会对退市币精确处理,速度慢；False 速度快,模糊处理
othCfg = {
    'log_level': 'INFO',
    'cal_factor_type': cal_factor_type,
    'hourly_details': hourly_details,
    'select_by_hour': select_by_hour,
    'filter_before_exec': filter_before_exec,
    'filter_after_exec': filter_after_exec,
    'start_date': start_date,
    'end_date': end_date,
    'factor_long_list': [],
    'factor_short_list': [],
    'trade_type': trade_type,
    'compound_name': compound_name,
    'quit_symbol_filter_hour': playCfg['hold_hour_num'][0],
    'p_signal_fun': p_signal_fun,
    'select_offsets': [long_select_offset, short_select_offset],
    'white_list': [long_white_list, short_white_list],
    'black_list': [long_black_list, short_black_list],
}
if playCfg['offset_stop_win'][0] != 0 or playCfg['offset_stop_loss'][0] != 0:
    assert playCfg['offset_stop_win'][0] > 0 and playCfg['offset_stop_loss'][0] < 0





def run():
    #提取读取数据时的所有组合
    product_list = list(product(ergodic_factor_list, ergodic_filter_list))
    # print(product_list)

    for factor_config,filter_config in product_list:
        #因子配置转为class
        factor_class_list = factor_config_transform_class(factor_config)
        #过滤配置转为class
        filter_class_list = filter_config_transform_class(filter_config)
        print(factor_class_list,filter_class_list)
        #读取数据
        df = reader.readhour(
            trade_type,
            factor_class_list,
            filter_class_list=filter_class_list)


        #读取所有数据还有因子数据以后,将数据中的选币因子数据和过滤因子数据分开,分成两个列表
        df_columns = df.columns.tolist()
        #df_columns里包含all_factor_calss_list字符串的列名
        feature_list = [x for x in df_columns if any([y in x for y in factor_config])]
        filter_list = [x for x in df_columns if any([y in x for y in filter_config])]
        #df = df[['candle_begin_time', 'symbol', 'close', 'avg_price']+feature_list + filter_list]
        
        if len(filter_list)==0:
            filter_list.append('None')
        # 因子分组
        factor_params_list = factor_list_transform_std_factor(feature_list=feature_list)
        filter_params_list = filter_list_transform_std_filter(filter_list)

        #获得所有的参数组合
        params_list = list(product(factor_params_list, filter_params_list))

        

        _othCfg_list = []
        #运行回放
        for params in params_list:
            _othCfg = othCfg.copy()
            _othCfg['factor_long_list'] = params[0]
            _othCfg['factor_short_list'] = params[0]
            if params[1] != ['None']:
                _othCfg['filter_before_exec'] = params[1]
            else:
                _othCfg['filter_before_exec'] = []
            _othCfg_list.append(_othCfg)
       
        # #串行回放
        # for _othCfg in _othCfg_list:
        #     run_play(df,playCfg, _othCfg)

        #并行回放
        njobs = os.cpu_count() - 2
        Parallel(n_jobs=njobs)(delayed(run_play)(df, playCfg, _othCfg)for _othCfg in _othCfg_list)


        del df





def main():
    if filter_before_exec:
        print('前置过滤源码：')
        [print(x, '\n') if tag in x else print(x) for x in filter_before_exec]
    if filter_after_exec:
        print('后置过滤源码：')
        [print(x, '\n') if tag in x else print(x) for x in filter_after_exec]
    res = playback_start(playCfg, othCfg)

if __name__ == '__main__':
    run()
    #main()
