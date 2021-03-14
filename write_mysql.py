import pymysql
import pandas as pd
import copy

def write_mysql(mysql_uri,table_name,df0):
    df = copy.copy(df0)
    # 数据库信息
    host = mysql_uri.split('/')[2].split('@')[1].split(':')[0]
    port = int(mysql_uri.split('/')[2].split('@')[1].split(':')[1])
    user = mysql_uri.split('/')[2].split('@')[0].split(':')[0]
    password = mysql_uri.split('/')[2].split('@')[0].split(':')[1]
    database = mysql_uri.split('/')[3]
    # 数据表信息
    df.reset_index(inplace=True)
    col = df.columns
    # 如果表不存在创建表
    db = pymysql.connect(host=host,port=port,user=user,password=password,database=database)
    cursor = db.cursor()
    # a = ["Time","('压缩机联端X', 'one_freq_x')","('压缩机联端X', 'one_freq_y')","('压缩机联端X', 'two_freq_x')","('压缩机联端X', 'two_freq_y')"]
    sql_col = ''
    for j in col:#cldata_target_lst['vib']
        if j == 'Time':
            sql_col += '`Time` datetime NOT NULL PRIMARY KEY'
        else:
            sql_col += ',`%s` Double'%str(j)
    sql = "CREATE TABLE IF NOT EXISTS "+table_name+" (%s)"%sql_col
    cursor.execute(sql)
    db.close()
    # 存入数据
    db = pymysql.connect(host=host,port=port,user=user,password=password,database=database)
    cursor = db.cursor()
    for i in df.index:
        sql_dfcol = ''
        for k in range(len(col)):
            if k == 0:
                sql_dfcol += "`%s`"%col[k]
            else:
                sql_dfcol += ",`%s`"%col[k]
        sql_value = ''
        value = df.loc[i,:]
        for k in range(len(value)):
            if k == 0:
                sql_value += "'%s'"%value[k]
            else:
                sql_value += ",'%s'"%value[k]
        print(value[0])
        sql = "INSERT IGNORE INTO "+table_name+" (%s) VALUES (%s)"%(sql_dfcol,sql_value)
        # print(sql)
        cursor.execute(sql)
    db.commit()
    db.close()