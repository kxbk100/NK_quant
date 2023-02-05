__Author__ = 'XMY'


def signal(*args):
    # ZH_涨跌幅
    df = args[0]
    n = args[1]
    factor_name = args[2]

    df.sort_values(by='candle_begin_time', inplace=True)  # 排序
    df.drop_duplicates(subset=['candle_begin_time'], inplace=True, keep='last')  # 去除重复值
    df.set_index('candle_begin_time', inplace=True)

    df1 = df.resample(f'{n}h').ffill()  # 转换为nh的数据
    df1 = df1.reindex(df.index, method='pad')  # 以原数据时间坐标合并回去
    df[factor_name] = (df['close'] - df1['close']) / df1['close']  # 计算非滚动涨跌幅

    df.reset_index(drop=False, inplace=True)  # 重置index

    # print(df)
    return df
