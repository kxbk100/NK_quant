from job import playback_start, playCfg, partial,backtest_single_factor_single_filter,data_processing
from function import filter_generate, parallel_filter_handle
from signals import *
from itertools import product
#导入cpu_count
from multiprocessing import cpu_count
from joblib import Parallel, delayed

import os
from glob import glob
import gc
from pprint import pprint

output_path = os.path.join('f1_1+1_output')
output_fa_path = os.path.join(output_path, '选币因子分类')
output_fi_path = os.path.join(output_path, '过滤因子分类')
if not os.path.exists(output_path):
	os.mkdir(output_path)
if not os.path.exists(output_fa_path):
	os.mkdir(output_fa_path)
if not os.path.exists(output_fi_path):
	os.mkdir(output_fi_path)




compound_name = '中性回放遍历1PRO'  # name
# ===常规配置
cal_factor_type = 'cross' # corss/ verical
# start_date = '2022-01-01'
start_date = '2021-01-01'
end_date = '2022-11-20'

filter_dict_config = {}
factor_list1 = []
factor_list2 = []

trade_type = 'swap'
playCfg['c_rate'] = 6 / 10000  # 手续费
playCfg['hold_hour_num'] = 12  # hold_hour
# 只跑指定的N个offset,空列表则全offset
long_select_offset = []
short_select_offset = []

# ===回放增强配置
playCfg['select_coin_num'] = 1  # 多头选币数
playCfg['sell_coin_num'] = 1  # 空头选币数
# 多币权重参数(不小于0),多币时有效，详见https://bbs.quantclass.cn/thread/8835
playCfg['buy_p'] = 0  # 0 :等权, 0 -> ∞ :多币头部集中度逐渐降低
playCfg['sell_p'] = 0  # select_coin_num = 3, buy_p = 1 ;rank 1,2,3 的资金分配 [0.43620858, 0.34568712, 0.21810429]
playCfg['leverage'] = 1  # 杠杆率
playCfg['long_risk_position'] = 0  # 多头风险暴露 0.1 对冲后 10% 净多头, -0.2 对冲后 20% 净空头
playCfg['initial_trade_usdt'] = 10000  # 初始投入，金额过小会导致某些币无法开仓

# 固定白名单 'BTCUSDT'
long_white_list = []
short_white_list = []
# 固定黑名单
long_black_list = []
short_black_list = []

# ===过滤配置(元素皆为字符串，仿照e.g.写即可 支持 & |)


filter_after_exec = []


#遍历因子列表,全量获取因子
factor_path_list = glob(os.path.join('src_backtesting', 'factors', '*.py'))
factor_list = [f.split(os.sep)[-1] for f in factor_path_list]
factor_name_list = [f[:-3] for f in factor_list if f != '__init__.py']
factor_name_list = sorted(factor_name_list)
factor_list = factor_name_list[:30]

#遍历因子列表,手写因子
factor_list = ['Bias']

#过滤因子
# filter_list = []


# 将默认的串联过滤转化为并联，只针对前置过滤且有使用了rank/pct类型的过滤集，value类型串并联无影响
# filter_before_exec = parallel_filter_handle(filter_before_exec)

# 后置过滤 在选币后下单前控制选定币种的资金分配系数
# filter_list = ['涨跌幅max']
# filter_info = """filter_factor = ['涨跌幅max_fl_24'][0]
# df1 = df1[df1[filter_factor]<0.2]
# df2 = df2[df2[filter_factor]<0.2]
# """
# filter_before_exec = [filter_info]


filter_list = ['涨跌幅max']
filter_before_exec = [
    """filter_factor = ['涨跌幅max_fl_24'][0]
df1 = df1[(df1[filter_factor] <= 0.2)]
df2 = df2[(df2[filter_factor] <= 0.2)]
"""
]

# ===花式配置
# 资金曲线择时:(param[-1] 默认为计算signal需要的最少小时数)
p_signal_fun = None
# param = [48, 48]
# p_signal_fun = partial(ma_signal, param)

# ===回放参数配置
hourly_details = False  # True 会生成详细截面数据 持仓面板和下单面板，耗时增加20S左右
select_by_hour = False  # True 为逐小时，会对退市币精确处理，速度慢；False 速度快，模糊处理

othCfg = {
    'cal_factor_type':cal_factor_type,
    'hourly_details': hourly_details,
    'select_by_hour': select_by_hour,
    'filter_before_exec': filter_before_exec,
    'filter_after_exec': filter_after_exec,
    'start_date': start_date,
    'end_date': end_date,
    'factor_list1': factor_list1,
    'factor_list2': factor_list2,
    'trade_type': trade_type,
    'compound_name': compound_name,
    'quit_symbol_filter_hour': playCfg['hold_hour_num'][0],
    'p_signal_fun':p_signal_fun,
    'select_offsets':[long_select_offset,short_select_offset],
    'white_list':[long_white_list,short_white_list],
    'black_list':[long_black_list,short_black_list]
}


def run():
    #因子列名转成标准格式
    def factor_cloums_to_convert(factor_list):
        factor_list_convert = []
        for factor in factor_list:
            split_list=factor.split('_bh_')
            
            if len(split_list)==2:
                if 'diff' not in split_list[1]:
                    factor_list_convert.append([split_list[0],True,split_list[1],0,1])
                    factor_list_convert.append([split_list[0],False,split_list[1],0,1])
                else:
                    factor_list_convert.append([split_list[0],True,split_list[1].split('_diff_')[0],split_list[1].split('_diff_')[1],1])
                    factor_list_convert.append([split_list[0],False,split_list[1].split('_diff_')[0],split_list[1].split('_diff_')[1],1])

            
        return factor_list_convert
    
    def filter_cloums_to_convert(filter_list):
        filter_list_convert = []
        for _filter in filter_list:
            split_list=_filter.split('_fl_')
            if len(split_list)==2:
                
                filter_message = filter_dict_config[split_list[0]].copy()
                filter_message['filter_factor'] = _filter
                filter_list_convert.append(filter_message)

        return filter_list_convert

    # 因子列表迪卡尔乘积，如果读取所有的因子那么非常耗时间
    for _factor in factor_list:
        #判断是否需要过滤
        df,factor_cloums,filter_cloums = data_processing([_factor],filter_list,playCfg, othCfg)

        factor_lists = factor_cloums_to_convert(factor_cloums)

        back_othcfg_list = []
        for _factor in factor_lists:
            _othCfg = othCfg.copy()
            _othCfg['factor_list1'] = [_factor]
            _othCfg['factor_list2'] = [_factor]            
            _othCfg['compound_name'] = _factor[0] + '_' + str(_factor[1]) + '_'+ _factor[2] + f'_diff_{_factor[3]}'
            back_othcfg_list.append(_othCfg)

        #串行
        # res_list = []
        # for back_othcfg in back_othcfg_list:
        #     res_list.append(backtest_single_factor_single_filter(df, playCfg, back_othcfg))
        #并行计算
        njobs = 3 #cpu_count() - 2
        Parallel(n_jobs=njobs)(delayed(backtest_single_factor_single_filter)(df, playCfg, back_othcfg)for back_othcfg in back_othcfg_list)
        del df
        gc.collect()


    
    

    

    


if __name__ == '__main__':    
    run()










