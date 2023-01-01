import numpy as np
import pandas as pd
def ma_signal(param, arr):
    # 通过返回trade_ratio达到择时效果，trade_ratio 绝对值最小为 0.1，当trade_ratio 为负时，有逆刀效果
    min_length = param[-1]
    n = param[0]
    if len(arr) < min_length:
        return 1
    else:
        if arr[-1] > np.nanmean(arr[-n:]):
            trade_ratio = 1
        else:
            trade_ratio = 0.2
    return trade_ratio