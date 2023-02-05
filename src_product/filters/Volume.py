#!/usr/bin/python3
# -*- coding: utf-8 -*-
def signal(*args):

    df = args[0]
    n  = args[1]
    factor_name = args[2]

    df[factor_name] = df['quote_volume'].rolling(n, min_periods=1).sum()

    return df