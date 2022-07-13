#!/usr/bin/python3
# -*- coding: utf-8 -*-

import os
import time
import pandas as pd
import numpy  as np
import geatpy as ea
from datetime import datetime, timedelta
from sklearn.linear_model import LassoCV

from utils import reader, tools, preprocess, ind


def gen_selected(df, select_coin_num):
    df1 = df.copy()
    df2 = df.copy()

    # 根据因子对比进行排名
    # 从小到大排序
    df1['排名1'] = df1.groupby('candle_begin_time')['因子'].rank(method='first')
    df1 = df1[(df1['排名1'] <= select_coin_num)]
    df1['方向'] = 1

    # 从大到小排序
    df2['排名2'] = df2.groupby('candle_begin_time')['因子'].rank(method='first', ascending=False)
    df2 = df2[(df2['排名2'] <= select_coin_num)]
    df2['方向'] = -1

    # 合并排序结果
    df = pd.concat([df1, df2], ignore_index=True)

    df = df[header_columns + ['方向']]
    df.sort_values(by=['candle_begin_time', '方向'], ascending=[True, False], inplace=True)
    df.reset_index(drop=True, inplace=True)

    return df


start_date = '2020-06-01'
split_date = '2021-02-01'
end_date   = '2021-09-01'

if_reverse = True
header_columns  = ['candle_begin_time', 'symbol', 'ret_next']
select_coin_num = 1
c_rate 			= 6/10000
trade_type 		= 'swap'
hold_hour  		= '6H'      
back_hour_list  = [3, 4, 6, 8, 9, 12, 24, 30, 36, 48, 60, 72, 96]   	 			
diff_list 		= [0, ] # 不使用差分
filter_list     = []
factor_classes  = [
    'Bias',
    'Cci',
]

factor_list = []
for factor_name in factor_classes:
    for back_hour in back_hour_list:
        for d_num in diff_list:
            factor_list.append((factor_name, True, back_hour, d_num, 1.0))
feature_list = tools.convert_to_feature(factor_list)

# ===读取数据
df = reader.readall(trade_type, hold_hour, factor_classes, date_range=(start_date, end_date))
# 删除某些行数据
df = df[df['volume'] > 0]  # 该周期不交易的币种
df.dropna(subset=['下个周期_avg_price'], inplace=True)  # 最后几行数据，下个周期_avg_price为空
# 筛选日期范围
df = df[df['candle_begin_time'] >= pd.to_datetime(start_date)]
df = df[df['candle_begin_time'] <= pd.to_datetime(end_date)]
# ===数据预处理
df['ret_next'] = df['下个周期_avg_price'] / df['avg_price'] - 1
df = df[header_columns + feature_list]
df = preprocess.preprocess_by_median(df, feature_list)
# ===划分训练集/测试集合
train = df[df['candle_begin_time'] <  pd.to_datetime(split_date)].reset_index(drop=True)
test  = df[df['candle_begin_time'] >= pd.to_datetime(split_date)].reset_index(drop=True)
print('数据处理完毕!!!\n')


# ===开始回归
selected_features = feature_list
# 标准化处理
train_std = dict()
for f in selected_features:
    min_thres, max_thres = np.quantile(train[f], [0.001, 0.999])
    train[f] = np.clip(train[f], min_thres, max_thres) 
    mean = train[f].mean()
    std  = train[f].std()
    train[f] -= mean
    train[f] /= std
    train_std[f] = std
min_thres, max_thres = np.quantile(train['ret_next'], [0.001, 0.999])
train['ret_next'] = np.clip(train['ret_next'], min_thres, max_thres)  
ret_std = train['ret_next'].std()
train['ret_next'] /= ret_std

model = LassoCV(max_iter=1000, n_jobs=-1, verbose=True)
model.fit(train[selected_features], train['ret_next'])
train_coef = pd.Series(model.coef_, index=selected_features)
train_std  = pd.Series(train_std)
coef_      = train_coef / train_std


# ===保存回归结果
result_dir = './output/lasso.txt'
if os.path.exists(result_dir):
    os.remove(result_dir)
results = []
for k, v in coef_.to_dict().items():
    if v == 0: continue
    factor = k.split('_bh_')[0]
    if '_diff_' in k:
        t = k.split('_diff_')
        d = t[1]
        b = int(t[0].split('_bh_')[1])
    else:
        b = int(k.split('_bh_')[1])
        d = 0
    results.append(f'''('{factor}', True, {b}, {d}, {v}),''')
with open(result_dir, 'w') as f:
    f.writelines(results)


# ===回测样本外数据
# 计算因子
test['linregr'] = test[selected_features].dot(coef_.T)
reverse_factor = -1 if if_reverse else 1
test['因子'] = reverse_factor * test['linregr']
# ===选币
select_coin = gen_selected(test, select_coin_num)
# ===计算涨跌幅
select_coin = tools.evaluate(select_coin, c_rate, select_coin_num)
# ===合并资金曲线
select_coin['本周期多空涨跌幅'] = select_coin['本周期多空涨跌幅']/int(hold_hour[:-1])
select_coin['资金曲线'] = (select_coin['本周期多空涨跌幅'] + 1).cumprod()
rtn, select_c = ind.cal_ind(select_coin)
print(rtn)
print('\n')
tools.plot(select_c, mdd_std=0.2)




