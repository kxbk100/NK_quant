#!/usr/bin/python3
# -*- coding: utf-8 -*-
import pandas as pd
import numpy as np
from utils.diff import add_diff

# https://bbs.quantclass.cn/thread/18768

def signal(*args):
    df = args[0]
    n  = args[1]
    factor_name = args[2]

    df[factor_name] = df['quote_volume'].rolling(24, min_periods=1).mean()/df['quote_volume'].rolling(24*n, min_periods=1).mean() -1

    return df
