import pandas as pd
import datetime

def read_mysql(sqlEngine,ID,col,starttime,endtime):
    data_lst = {}
    for i in col:
        table_name = 'id'+ID+'pt_'+i.lower()
        data = pd.read_sql("select * from  "+table_name+" WHERE `Time` between '%s' and '%s'"%(starttime,endtime) , con=sqlEngine, index_col=None)
        data['Time'] = pd.to_datetime(data['Time'],format='%Y-%m-%d %H:%M:%S')
        data.set_index('Time',inplace=True)
        data = data[col[i]]
        # data = data.loc[starttime:endtime]
        #print(data)
        data_lst[i] = data
    return data_lst
def read_mysql_pre(sqlEngine,ID,col,starttime,endtime):
    data_lst = {}
    starttime = str(pd.to_datetime(starttime)-datetime.timedelta(hours=2))
    print('测试数据开始时间:'+starttime)
    print('测试数据结束时间:'+endtime)
    for i in col:
        table_name = 'id'+ID+'pt_'+i.lower()
        data = pd.read_sql("select * from  "+table_name+" WHERE `Time` between '%s' and '%s'"%(starttime,endtime) , con=sqlEngine, index_col=None)
        data['Time'] = pd.to_datetime(data['Time'],format='%Y-%m-%d %H:%M:%S')
        data.set_index('Time',inplace=True)
        data = data[col[i]]
        # data = data.loc[starttime:endtime]
        #print(data)
        data_lst[i] = data
    return data_lst
def read_resid(sqlEngine,ID,col,starttime,endtime):
    data_lst = {}
    for i in col:
        table_name = 'id'+ID+'pt_'+i.lower()
        data = pd.read_sql("select * from  "+table_name , con=sqlEngine, index_col=None)
        data['Time'] = pd.to_datetime(data['Time'],format='%Y-%m-%d %H:%M:%S')
        data.set_index('Time',inplace=True)
        data = data[col[i]]
        data = data.loc[starttime:endtime]
        #print(data)
        data_lst[i] = data
    return data_lst
