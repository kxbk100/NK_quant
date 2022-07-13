#!/usr/bin/python3
# -*- coding: utf-8 -*-

import pandas as pd
import numpy  as np


def signal(*args):
	# BBW
	df = args[0]
	params = args[1]
	factor_name = args[2]

	n = int(params[0])
	m = int(params[1])

	df['median']  = df['close'].rolling(n, min_periods=1).mean()
	df['std']     = df['close'].rolling(n, min_periods=1).std(ddof=0)

	df['up'] = df['median'] + df['std'] * m
	df['dn'] = df['median'] - df['std'] * m

	df['bbw'] = (df['up']-df['dn'])/df['median']
	df[factor_name] = df['bbw']

	return df


