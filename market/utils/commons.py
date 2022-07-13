#!/usr/bin/python3
# -*- coding: utf-8 -*-

import os
import time
import glob
import shutil
import pandas as pd
import numpy  as np
from datetime import datetime, timedelta


def run_until_success(func, tryTimes=5, sleepTimes=60):
    error_msg = None
    retry  = 0
    while True:
        if retry <= tryTimes:
            try:
                result = func()
                return [result, retry]
            except (Exception) as reason:
                error_msg = reason
                retry += 1
                if sleepTimes != 0:
                    time.sleep(sleepTimes)  # 一分钟请求20次以内
        else:
            break

    if error_msg:
        print(error_msg)

    return False


def robust(actual_do, *args, **keyargs):
    tryTimes    = 5
    sleepTimes  = 1

    result = run_until_success(
        func=lambda: actual_do(*args, **keyargs), 
        tryTimes=tryTimes, 
        sleepTimes=sleepTimes
    )
    if result:
        return result[0]
    else:
        os._exit(0)    


