#!/usr/bin/python3
# -*- coding: utf-8 -*-
import pandas as pd
import numpy as np
from utils.diff import add_diff


def signal(*args):
    # Volume
    df = args[0]
    n = args[1]
    factor_name = args[2]
    
    df[factor_name] = df['quote_volume'].rolling(n, min_periods=1).sum()

    return df
