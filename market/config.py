#!/usr/bin/python3
# -*- coding: utf-8 -*-

import os
import time
import glob
import shutil
import pandas as pd
import numpy  as np
from datetime import datetime, timedelta


_ = os.path.abspath(os.path.dirname(__file__))  		# 返回当前文件路径
root_path = os.path.abspath(os.path.join(_, '..'))  	# 返回根目录文件夹


header_columns = [
    'candle_begin_time', 
    'open', 
    'high', 
    'low', 
    'close', 
    'volume', 
    'quote_volume',
    'trade_num',
    'taker_buy_base_asset_volume', 
    'taker_buy_quote_asset_volume',
]