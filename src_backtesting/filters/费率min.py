#!/usr/bin/python3
# -*- coding: utf-8 -*-

import pandas as pd
import numpy  as np


def signal(*args):
	df = args[0]
	n  = args[1]
	factor_name = args[2]
	df[factor_name]=df['fundingRate'].rolling(n).min()
	return df