import akshare as ak
import numpy as np
import pandas as pd
import warnings
import mysql.connector
import pandas.errors
from sqlalchemy import create_engine
from datetime import datetime, date, timedelta
from tradedate import *
from selfdefinfunc import Search_NavDate
from chinese_calendar import is_workday
from var import *

warnings.filterwarnings('ignore')

login1 = ['localhost','root','hean0523','3306','test']
login2 = ['sh-cynosdbmysql-grp-9s19v44w.sql.tencentcdb.com','Hean_intern','Hean_intern','27974','实习生专属数据库']
login3 = ['148.135.124.238','dorisosh','hean0523','3306','test']

var_init()
set_value('final_text', "")

# 获取/更新指数历史数据
class Index_Data():
    def __init__(self):
        self.name_code = {'沪深300指数':'sh000300','中证500指数':'sh000905','中证1000指数':'sh000852','国证2000指数':'sz399303'}
    @property
    def index_data(self):
        all_index_data = pd.DataFrame()
        for index_name in list(self.name_code.keys()):
            temp_data = ak.stock_zh_index_daily_em(symbol=self.name_code[index_name])
            temp_data.drop(columns=['open','high','low','volume','amount'],inplace=True)
            temp_data.rename(columns={'date':'日期','close':index_name},inplace=True)
            try:
                all_index_data = pd.merge(all_index_data, temp_data, how='outer')
            except pandas.errors.MergeError:
                all_index_data = temp_data
        all_index_data['日期'] = all_index_data['日期'].apply(lambda x:datetime.date(datetime.strptime(x,'%Y-%m-%d')))
        return all_index_data

class Database():
    def __init__(self, login):
        self.login = login
        self.cnx = mysql.connector.connect(host=self.login[0], user=self.login[1], password=self.login[2], port=self.login[3], database=self.login[4])
        self.engine = create_engine(f'mysql+mysqlconnector://{self.login[1]}:{self.login[2]}@{self.login[0]}:{self.login[3]}/{self.login[4]}')
        self.cursor = self.cnx.cursor()
    @property
    def tables(self):
        self.cursor.execute("SHOW TABLES")
        return self.cursor.fetchall()

    def if_exist(self, name):
        for table in self.tables:
            if name == table[0]:
                return True
        return False
    def wt_data(self, add, name, method):
        add.to_sql(name=name, con=self.engine, if_exists=method, index=False)
    def rd_data(self, name):
        read = pd.read_sql_query("SELECT * FROM %s" % name, self.engine)
        return read

def rem_dup(data0):
    data = data0.copy()
    fund_list = data['基金名称'].values.tolist()
    temp_list = map(lambda x: x[:-1], fund_list)
    fund_list_no_dup = list(set(temp_list))
    for fund_name in fund_list_no_dup:
        if ((fund_name + 'C') in fund_list):
            data.drop(data[data['基金名称'] == (fund_name + 'C')].index, inplace=True)
        elif ((fund_name + 'A') in fund_list):
            data.drop(data[data['基金名称'] == (fund_name + 'A')].index, inplace=True)
        else:
            continue
    data.reset_index(drop=True,inplace=True)
    return data

index_list = ['沪深300', '中证500', '中证1000', '国证2000']

def deal(name, add):
    for element in index_list:
        if element in name:
            return element + add
        else:
            continue
    return '-'

def product_init(database):
    def found_size(code):
        try:
            temp_info = ak.fund_individual_basic_info_xq(symbol=code)
            return [temp_info.iloc[3, 1],temp_info.iloc[4, 1]]
        except KeyError:
            return [np.nan,np.nan]
    fund_info = ak.fund_info_index_em(symbol="股票指数", indicator="增强指数型")
    fund_info.drop(labels=['近1周', '近1月', '近3月', '近6月', '近2年', '近3年', '今年来', '手续费', '起购金额','跟踪标的', '跟踪方式'], axis=1, inplace=True)
    fund_info = rem_dup(fund_info)
    try:
        fund_info['策略类型'] = fund_info['基金名称'].map(lambda x:deal(x,'指增'))
    except KeyError:
        fund_info['策略类型'] = '-'
    fund_info.reset_index(drop=True,inplace=True)
    fund_info['暂时'] = fund_info['基金代码'].map(lambda x: found_size(x))
    fund_info['成立时间'] = fund_info['暂时'].map(lambda x:x[0])
    fund_info['最新规模'] = fund_info['基金代码'].map(lambda x:x[1])
    fund_info.drop(columns=['暂时'], inplace=True)
    fund_info.rename(columns={'日期':'净值日期'},inplace=True)
    '''for i in range(len(fund_info)):
        try:
            fund_info.loc[i,'成立时间'] = datetime.date(datetime.strptime(fund_info.loc[i,'成立时间'], '%Y-%m-%d'))
        except ValueError:
            continue'''
    # fund_info['是否刷新净值'] = np.nan
    # fund_info.loc[:, '是否刷新净值'] = '是'
    fund_info = fund_info[['基金代码', '基金名称', '单位净值', '净值日期','策略类型','成立时间', '最新规模', '日增长率', '近1年', '成立来']]
    database.wt_data(fund_info, '产品策略对照表', 'replace')
    print('产品策略对照表初始化完成')
    set_value('final_text', get_value('final_text') + "产品策略对照表初始化完成\n")
    return 0

def isTradeDay(date):
    if is_workday(date):
        if datetime.isoweekday(date) < 6:
            return True
    return False

def product_update(database):
    try:
        product_list = database.rd_data('产品策略对照表')
    except:
        product_init(database)
        return 0
    last_date = datetime.strptime(product_list.loc[0,'净值日期'], '%Y-%m-%d')
    today_date = datetime.today()
    if today_date > last_date and isTradeDay(today_date):
        fund_info1 = ak.fund_info_index_em(symbol="沪深指数", indicator="增强指数型")
        fund_info1 = rem_dup(fund_info1)
        if len(fund_info1) > len(product_list):
            product_init(database)
            print('发现基金产品增加，产品策略对照表重构完成')
            set_value('final_text', get_value('final_text') + "发现基金产品增加，产品策略对照表重构完成\n")
            return 0
        else:
            fund_info1 = ak.fund_info_index_em(symbol="股票指数", indicator="增强指数型")
            fund_info1.rename(columns={'日期':'净值日期'}, inplace=True)
            fund_info1 = fund_info1[['基金代码','单位净值','净值日期']]
            product_list.drop(columns=['单位净值','净值日期'], inplace=True)
            product_list = pd.merge(product_list, fund_info1, on='基金代码')
            database.wt_data(product_list, '产品策略对照表', 'replace')
            print('基金产品无变化，基金净值已更新')
            set_value('final_text', get_value('final_text') + "基金产品无变化，基金净值已更新\n")
            return 0
    else:
        print('产品策略对照表无需更新')
        set_value('final_text', get_value('final_text') + "产品策略对照表无需更新\n")
        return 0

class Fund():
    database = Database(login3)
    def __init__(self, name, code, index):
        self.code = code
        self.name = name
        self.index = index
        all_index = Index_Data()
        self.index_value = all_index.index_data[['日期', self.index]].rename(columns={'日期':'净值日期'})
    @property
    def get_fund_values(self):
        net = ak.fund_open_fund_info_em(symbol=self.code, indicator="单位净值走势")
        net = net[['净值日期','单位净值']]
        cum = ak.fund_open_fund_info_em(symbol=self.code, indicator="累计净值走势")
        div_flag = np.diff(cum['累计净值'].tolist()) - np.diff(net['单位净值'].tolist())
        fund_values = pd.merge(net, cum, on='净值日期')
        # fund_values['单位净值*'], fund_values['累计净值*'] = fund_values.shift()['单位净值'], fund_values.shift()['累计净值']
        # fund_values['红利'] = (fund_values['累计净值*']-fund_values['累计净值']) - (fund_values['单位净值*']-fund_values['单位净值'])
        # fund_values['复权因子'] = fund_values['单位净值*']/(fund_values['单位净值*']-fund_values['红利'])
        fund_values['复权因子'] = 1.0000
        for i in range(1, len(div_flag)):
            fund_values.loc[i,'复权因子'] = (1 + div_flag[i]/fund_values.loc[i,'单位净值']) * fund_values.loc[i-1,'复权因子']
        fund_values['复权净值'] = fund_values['复权因子'] * fund_values['单位净值']
        return fund_values
    @property
    def found(self):
        return self.get_fund_values.loc[0,'净值日期']
    @property
    def found_cal(self):
        fund_values = self.get_fund_values
        # fund_values.drop(columns=['累计净值','复权因子'],inplace=True)
        fund_values = pd.merge(fund_values,self.index_value,on='净值日期')
        fund_values.rename(columns={self.index:'对标指数'},inplace=True)
        # fund_values.loc[0, ['成立以来收益率', '对标指数成立以来收益率', '成立以来超额（除法）', '成立以来最大回撤']] = 0
        fund_values['成立以来收益率'] = fund_values['复权净值'] / fund_values.loc[0,'复权净值'] - 1
        fund_values['对标指数成立以来收益率'] = fund_values['对标指数'] / fund_values.loc[0,'对标指数'] - 1
        fund_values['成立以来超额（除法）'] = (fund_values['成立以来收益率']+1)/(fund_values['对标指数成立以来收益率']+1)-1
        fund_values['成立以来最大回撤'] = fund_values['成立以来收益率'].apply(lambda x: (x+1)/(fund_values.loc[:fund_values[fund_values.成立以来收益率==x].index.tolist()[0],'成立以来收益率'].max()+1)-1)
        fund_values['成立以来超额'] = fund_values['成立以来超额（除法）']
        # print(self.name)
        # print(fund_values[['净值日期','复权净值','成立以来收益率']])
        try:
            fund_values['成立以来最大回撤（超额）'] = fund_values['成立以来超额'].apply(lambda x: (x+1)/(fund_values.loc[:fund_values[fund_values.成立以来超额==x].index.tolist()[0],'成立以来超额'].max()+1)-1)
        except:
            fund_values['成立以来最大回撤（超额）'] = np.nan
        fund_values.drop(columns=['成立以来超额'],inplace=True)
        return fund_values
    @property
    def recent_cal(self):
        columns_to_add = [
            "近四周收益率", "对标指数近四周收益率", "近四周超额（除法）",
            "今年以来收益率", "对标指数今年以来收益率", "今年以来超额（除法）", "今年以来最大回撤", "今年以来最大回撤（超额）",
            "近三月收益率", "对标指数近三月收益率", "近三月超额（除法）", "近三月最大回撤", "近三月最大回撤（超额）",
            "近六月收益率", "对标指数近六月收益率", "近六月超额（除法）", "近六月最大回撤", "近六月最大回撤（超额）",
            "近一年收益率", "对标指数近一年收益率", "近一年超额（除法）", "近一年最大回撤", "近一年最大回撤（超额）",
            "近两年收益率", "对标指数近两年收益率", "近两年超额（除法）", "近两年最大回撤", "近两年最大回撤（超额）",
            "近三年收益率", "对标指数近三年收益率", "近三年超额（除法）", "近三年最大回撤", "近三年最大回撤（超额）",
        ]
        time_intervals = {'近四周':[datetime.date(time_last_Friday_time),time_time_last_Friday_lag4_time], '今年以来':[time_year_first,time_year_first_time], '近三月':[time_lag3_month,time_lag3_month_time], '近六月':[time_lag6_month,time_lag6_month_time], '近一年':[time_lag_year,time_lag_year_time], '近两年':[time_lag2_year,time_lag2_year_time], '近三年':[time_lag3_year,time_lag3_year_time]}
        list_chanpin_info = self.found_cal
        for interval in list(time_intervals.keys()):
            date_start = Search_NavDate(list_chanpin_info, datetime.date(time_intervals[interval][1]), self.found, time_time_last_Friday_lag4_time)
            # 是否符合成立区间判定 #
            if self.found <= datetime.date(time_intervals[interval][1]):
                # 计算收益序列 #
                mask = list_chanpin_info['净值日期'] >= date_start
                netvalue_lag_chanpin = float((list_chanpin_info.loc[list_chanpin_info['净值日期'] == date_start, ['复权净值']]).values[0][0])
                list_chanpin_info[f'{interval}收益率'] = list_chanpin_info.loc[mask].loc[:,'复权净值'] / netvalue_lag_chanpin - 1
                # 计算对标指数收益率/超额（除法）序列 #
                index_netvalue_lag_chanpin = float((list_chanpin_info.loc[list_chanpin_info['净值日期'] == date_start, ['对标指数']]).values[0][0])
                list_chanpin_info[f'对标指数{interval}收益率'] = list_chanpin_info.loc[mask].loc[:,'对标指数'] / index_netvalue_lag_chanpin - 1
                list_chanpin_info[f'{interval}超额（除法）'] = list_chanpin_info.apply(lambda x: (x[f'{interval}收益率'] + 1) / (x[f'对标指数{interval}收益率'] + 1) - 1, axis=1)
                # list_chanpin_info[f'{interval}超额'] = list_chanpin_info[f'{interval}超额（除法）']
                if interval != '近四周':
                    '''list_chanpin_info[f'{interval}最大回撤'] = list_chanpin_info[f'{interval}收益率'].apply(lambda x: (x + 1) / (list_chanpin_info.loc[:list_chanpin_info[list_chanpin_info.eval(interval+'收益率') == x].index.tolist()[0],f'{interval}收益率'].max() + 1) - 1)
                    list_chanpin_info[f'{interval}最大回撤1'] = list_chanpin_info[f'{interval}超额'].apply(lambda x: (x + 1) / (list_chanpin_info.loc[:list_chanpin_info[list_chanpin_info.eval(interval+'超额') == x].index.tolist()[0],f'{interval}超额'].max() + 1) - 1)
                    list_chanpin_info.drop(columns=[f'{interval}超额'],inplace=True)
                    list_chanpin_info.rename(columns={f'{interval}最大回撤1':f'{interval}最大回撤（超额）'},inplace=True)'''
                    nav_series = list_chanpin_info.loc[mask, f'{interval}收益率']
                    list_chanpin_info.loc[nav_series.index[0], f'{interval}最大回撤'] = 0
                    for d in range(1, len(nav_series)):
                        list_chanpin_info.loc[nav_series.index[d], f'{interval}最大回撤'] = (nav_series.iloc[d] + 1) / (max(nav_series.iloc[0:d + 1]) + 1) - 1
                        if list_chanpin_info.loc[nav_series.index[d], f'{interval}最大回撤'] > 0:
                            list_chanpin_info.loc[nav_series.index[d], f'{interval}最大回撤'] = 0
                    nav_series = list_chanpin_info.loc[mask, f'{interval}超额（除法）']
                    list_chanpin_info.loc[nav_series.index[0], f'{interval}最大回撤（超额）'] = 0
                    for d in range(1, len(nav_series)):
                        list_chanpin_info.loc[nav_series.index[d], f'{interval}最大回撤（超额）'] = (nav_series.iloc[d] + 1) / (max(nav_series.iloc[0:d + 1]) + 1) - 1
                        if list_chanpin_info.loc[nav_series.index[d], f'{interval}最大回撤（超额）'] > 0:
                            list_chanpin_info.loc[nav_series.index[d], f'{interval}最大回撤（超额）'] = 0
        return list_chanpin_info
    def update_in_db(self):
        Fund.database.wt_data(self.recent_cal, self.name.upper(), 'replace')
        print(self.name+'已存入数据库')
        set_value('final_text', get_value('final_text') + f"{self.name}已存入数据库\n")
        return 0

def each_product(database):
    product_list = database.rd_data('产品策略对照表')
    for i in range(5):
        if product_list.loc[i,'策略类型'] != '-':
            name,code,index = product_list.loc[i,'基金名称'],product_list.loc[i,'基金代码'],deal(product_list.loc[i,'基金名称'],'指数')
            if database.if_exist(name):
                fund = Fund(name,code,index)
                fund.update_in_db()
    print('已完成全部产品表更新')
    set_value('final_text',get_value('final_text')+"已完成全部产品表更新\n")
    return 0

