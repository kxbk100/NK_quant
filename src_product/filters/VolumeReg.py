#!/usr/bin/python3
# -*- coding: utf-8 -*-
import pandas as pd
import numpy as np
from utils.diff import add_diff
import talib as ta

# https://bbs.quantclass.cn/thread/18763

def signal(*args):
    # VolumeReg
    df = args[0]
    n = args[1]
    factor_name = args[2]

    df[factor_name] = ta.LINEARREG(df['quote_volume'], timeperiod=n)

    return df