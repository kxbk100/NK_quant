#!/usr/bin/python3
# -*- coding: utf-8 -*-

import pandas as pd
import numpy  as np


def signal(*args):
	# ZH_涨跌幅
	df = args[0]
	n  = args[1]
	factor_name = args[2]

	df[factor_name] = df['close'].pct_change(n)

	return df