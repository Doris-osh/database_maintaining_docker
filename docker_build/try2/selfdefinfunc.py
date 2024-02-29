# -*- coding: utf-8 -*-
"""
Created on Sat Feb 18 22:49:04 2023

@author: wanghui
"""
import numpy as np
import pandas as pd
import calendar
from datetime import datetime, date, timedelta
from chinese_calendar import is_workday

# 目标产品对应净值日期搜索函数
def Search_NavDate(df, dt, date_found_chanpin, time_time_last_Friday_lag4_time):
    def Recent_TradeDay(date):
        for i in range(10):
            if (is_workday(date) == False) or (date.weekday() == 5) or (date.weekday() == 6):
                date = date + timedelta(days=-1)
            else:
                break
        return date

    # 产品目标净值日检索 #
    if date_found_chanpin <= dt:
        temp = df.loc[(df['净值日期'] <= dt)]
        m= -1 
        last_date = temp.values[m][0]
        while Recent_TradeDay(temp.values[m][0]) != temp.values[m][0]:
            m = m - 1
        # 确定去年最后一个净值日是否为交易日 #
        m = m - 1
        while is_workday(temp.values[m+1][0]) == True:
            if dt == time_time_last_Friday_lag4_time:
                time_chanpin = temp.values[m+1][0]
            else:
                time_chanpin = temp.values[m+1][0]
            if time_chanpin == Recent_TradeDay(time_chanpin):
                break
            else:            
                m = m - 1
    else:
        time_chanpin = ''
    return time_chanpin