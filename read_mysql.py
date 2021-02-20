import pandas as pd
import datetime

def read_mysql(sqlEngine,ID,col,starttime,endtime):
    data_lst = {}
    for i in col:
        table_name = 'id'+ID+'pt_'+i.lower()
        data = pd.read_sql("select * from  "+table_name , con=sqlEngine, index_col=None)
        data['Time'] = pd.to_datetime(data['Time'],format='%Y-%m-%d %H:%M:%S')
        print(type(data['Time']))
        data = data.set_index('Time')
        print(type(data.index))
        print(col[i])
        #print(type(data['Time']))
        #print(starttime)
        data = data[col[i]]
        #starttime = datetime.datetime.strptime(starttime, "%Y-%m-%d %H:%M:%S")
        #endtime = datetime.datetime.strptime(endtime, "%Y-%m-%d %H:%M:%S")
        data = data[starttime:endtime]
        data_lst[i] = data
    return data_lst
