#!/usr/bin/python3
# -*- coding: utf-8 -*-

import pandas as pd
import numpy  as np


def signal(*args):
	# AdaptBolling
	df = args[0]
	n  = args[1]
	factor_name = args[2]

	df['median']  = df['close'].rolling(n, min_periods=1).mean()
	df['std']     = df['close'].rolling(n, min_periods=1).std(ddof=0)
	df['z_score'] = abs(df['close'] - df['median']) / df['std']
	
	df['m']  = df['z_score'].rolling(n, min_periods=1).max().shift(1)
	df['up'] = df['median'] + df['std'] * df['m']
	df['dn'] = df['median'] - df['std'] * df['m']

	df['distance'] = 0
	
	condition1 = df['close'] > df['up']
	condition2 = df['close'] < df['dn']

	df.loc[condition1, 'distance'] =  1
	df.loc[condition2, 'distance'] = -1

	df[factor_name] = df['distance']

	return df


