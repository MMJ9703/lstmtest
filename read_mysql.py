import pandas as pd

def read_mysql(sqlEngine,ID,col,starttime,endtime):
    data_lst = {}
    for i in col:
        table_name = 'id'+ID+'pt_'+i.lower()
        data = pd.read_sql("select * from  "+table_name , con=sqlEngine, index_col=None)
        data['Time'] = pd.to_datetime(data['Time'],format='%Y-%m-%d %H:%M:%S')
        data = data.set_index('Time')
        print(col[i])
        print(type(starttime))
        print(starttime)
        data = data[col[i]]
        starttime = pd.to_datetime(starttime,format='%Y-%m-%d %H:%M:%S')
        endtime = pd.to_datetime(endtime,format='%Y-%m-%d %H:%M:%S')
        data = data[starttime:endtime]
        data_lst[i] = data
    return data_lst
