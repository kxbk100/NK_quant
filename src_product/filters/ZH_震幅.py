#!/usr/bin/python3
# -*- coding: utf-8 -*-

import pandas as pd
import numpy  as np


def signal(*args):
	# ZH_震幅
	df = args[0]
	n  = args[1]
	factor_name = args[2]

	high = df['high'].rolling(n, min_periods=1).max()
	low  = df['low'].rolling(n,  min_periods=1).min()

	df[factor_name] = (high / low - 1)

	return df
