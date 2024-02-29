# -*- coding: utf-8 -*-
"""
Created on Sat Feb 18 19:17:34 2023

@author: wanghui
"""
import numpy as np
import pandas as pd
import calendar
from datetime import datetime, date, timedelta
from chinese_calendar import is_workday
from dateutil.relativedelta import relativedelta


def TradeDate():
    global time_last_month, time_last_year, time_this_Friday, time_last_Friday, time_last_Friday_lag1, time_latest_Friday, time_last_Friday_lag2, time_last_Friday_lag3, time_last_Friday_lag4
    global time_lag3_month, time_lag6_month, time_lag_year, time_lag2_year, time_lag3_year, time_year_first, time_last_month_lag1, time_last_Friday_time
    global time_year_first_time, time_last_month_time, time_lag3_month_time, time_lag6_month_time, time_last_year_time, time_lag_year_time, time_lag2_year_time, time_lag3_year_time, time_time_last_Friday_lag4_time, time_last_Friday_time, time_this_Friday_time
    def Recent_TradeDay(date):
        for i in range(10):
            if (is_workday(date) == False) or (date.weekday() == 5) or (date.weekday() == 6):
                date = date + timedelta(days=-1)
            else:
                break
        return date
    time_last_month = Recent_TradeDay(date.today() + timedelta(days=-date.today().day))
    time_last_Friday = Recent_TradeDay(date.today() + timedelta(days = -3 -date.weekday(date.today())))
    
    # 判断当前日期是否在今年的第一周内
    # 获取今年的第一天
    first_day_of_year = date(date.today().year, 1, 1)
    if date.today() <= (first_day_of_year + timedelta(days=(6 - first_day_of_year.weekday()))):
        # 如果是第一周内，则取去年的最后一个交易日
        time_last_year = Recent_TradeDay(date(date.today().year - 2, 12, 31))
    else:
        # 否则取去年的最后一个交易日
        time_last_year = Recent_TradeDay(date(date.today().year - 1, 12, 31))
        
    if date.today() == 4:
        time_this_Friday = Recent_TradeDay(date.today())
    elif date.today() == 7:
        time_this_Friday = Recent_TradeDay(date.today() + timedelta(days = 4))
    else:
        time_this_Friday = Recent_TradeDay(date.today() + timedelta(days = 4 -date.weekday(date.today())))
    if date.today() > time_this_Friday:
        time_latest_Friday = Recent_TradeDay(time_this_Friday)
    else:
        time_latest_Friday = Recent_TradeDay(time_last_Friday)
    
    n=1
    spread = timedelta(days=1)
    while spread == timedelta(days=1):
        time_last_Friday_lag1 = Recent_TradeDay(time_last_Friday + timedelta(days = -n))
        spread = Recent_TradeDay(time_last_Friday_lag1 + timedelta(days = 1)) - time_last_Friday_lag1
        n = n + 1
    
    n=1
    spread = timedelta(days=1)
    while spread == timedelta(days=1):
        time_last_Friday_lag2 = Recent_TradeDay(time_last_Friday_lag1 + timedelta(days = -n))
        spread = Recent_TradeDay(time_last_Friday_lag2 + timedelta(days = 1)) - time_last_Friday_lag2
        n = n + 1
    
    n=1
    spread = timedelta(days=1)
    while spread == timedelta(days=1):
        time_last_Friday_lag3 = Recent_TradeDay(time_last_Friday_lag2 + timedelta(days = -n))
        spread = Recent_TradeDay(time_last_Friday_lag3 + timedelta(days = 1)) - time_last_Friday_lag3
        n = n + 1
    
    n=1
    spread = timedelta(days=1)
    while spread == timedelta(days=1):
        time_last_Friday_lag4 = Recent_TradeDay(time_last_Friday_lag3 + timedelta(days = -n))
        spread = Recent_TradeDay(time_last_Friday_lag4 + timedelta(days = 1)) - time_last_Friday_lag4
        n = n + 1
    
    # 近三月交易日
    time_lag3_month = Recent_TradeDay(time_last_Friday + timedelta(days=-91))
    # 近六月交易日
    time_lag6_month = Recent_TradeDay(time_last_Friday + timedelta(days=-182))     
    # 近一年交易日
    time_lag_year = Recent_TradeDay(time_last_Friday + timedelta(days=-364))        
    # 近两年交易日
    time_lag2_year = Recent_TradeDay(time_last_Friday + timedelta(days=-729))    
    # 近三年交易日
    time_lag3_year = Recent_TradeDay(time_last_Friday + timedelta(days=-1094))    
    # 本年第一个交易日 #
    if time_last_month.month != 12:
        time_year_first = date(date.today().year, 1, 1)
    if time_last_month.month == 12:
       time_year_first = date(date.today().year - 1, 1, 1) 
    for i in range(7):
        if (is_workday(time_year_first) == False) or (time_year_first.weekday == 5) or (time_year_first.weekday == 6):
            time_year_first = time_year_first + timedelta(days = 1)
        else:
            break    
    # 上上月底交易日 #
    time_last_month_lag1 = Recent_TradeDay(time_last_month + timedelta(days=-time_last_month.day))
    # 转化时间单位 #
    time_this_Friday_time = datetime.strptime(str(time_this_Friday), '%Y-%m-%d')
    time_last_Friday_time = datetime.strptime(str(time_last_Friday), '%Y-%m-%d')
    time_last_month_time = datetime.strptime(str(time_last_month), '%Y-%m-%d')
    time_year_first_time = datetime.strptime(str(time_year_first), '%Y-%m-%d')
    time_last_year_time = datetime.strptime(str(time_last_year), '%Y-%m-%d')
    time_lag3_month_time = datetime.strptime(str(time_lag3_month), '%Y-%m-%d')
    time_lag6_month_time = datetime.strptime(str(time_lag6_month), '%Y-%m-%d')
    time_lag_year_time = datetime.strptime(str(time_lag_year), '%Y-%m-%d')
    time_lag2_year_time = datetime.strptime(str(time_lag2_year), '%Y-%m-%d')
    time_lag3_year_time = datetime.strptime(str(time_lag3_year), '%Y-%m-%d')
    time_time_last_Friday_lag4_time = datetime.strptime(str(time_last_Friday_lag4), '%Y-%m-%d')
    if (date.today() - time_last_Friday) >= timedelta(days=3):
        time_last_Friday_time = datetime.strptime(str(time_last_Friday), '%Y-%m-%d') 
    else:
        time_last_Friday_time = datetime.strptime(str(time_last_Friday), '%Y-%m-%d') + timedelta(days=-7)
    
    return

TradeDate()

def Indiv_TradeDate(date):
    def Recent_TradeDay(date):
        for i in range(10):
            if (is_workday(date) == False) or (date.weekday() == 5) or (date.weekday() == 6):
                date = date + timedelta(days=-1)
            else:
                break
        return date

    # Calculate one month ago
    one_month_ago = Recent_TradeDay(date - relativedelta(months=1))

    # Calculate three months ago
    three_months_ago = Recent_TradeDay(date - relativedelta(months=3))

    # Calculate one year ago
    one_year_ago = Recent_TradeDay(date - relativedelta(years=1))

    # Calculate two years ago
    two_years_ago = Recent_TradeDay(date - relativedelta(years=2))

    # Calculate three years ago
    three_years_ago = Recent_TradeDay(date - relativedelta(years=3))

    # Return the dates in datetime format
    return {
        "one_month_ago": one_month_ago,
        "three_months_ago": three_months_ago,
        "one_year_ago": one_year_ago,
        "two_years_ago": two_years_ago,
        "three_years_ago": three_years_ago
    }


