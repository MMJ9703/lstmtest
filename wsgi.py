# 添加所需要的库
import os
from pathlib import Path
import json
import time
import pymysql
import pandas as pd
# 添加自己编写的算法
from data_process import data_process
from read_mysql import read_mysql,read_mysql_pre
from lstm_model import train_lstm,predict_lstm,Bayes_pi,Bayes_pi_t
from write_mysql import write_mysql
# Web应用程序设置
from flask_executor import Executor


from threading import Thread
from sqlalchemy import create_engine


from flask import Flask
from flask import request
from flask_cors import *

application = Flask(__name__)
executor = Executor(application)
# 获取mysql环境变量
env = os.environ
# MYSQL_URI   mysql+pymysql://test:test@172.30.238.185:3306/test
mysql_uri = env.get('MYSQL_URI')

sqlEngine = create_engine(mysql_uri, pool_recycle=3600)

print ('=== mysql uri: ' + mysql_uri)

def trainlstm():
    try:

        # 读取信息
        param = request.get_json()
        saveConfigToFile(param)
        ID = param["param"]["id"]
        col_input = param["param"]["input"]
        col_target = param["param"]["target"]
        starttime = param["param"]["datastarttime"]
        endtime = param["param"]["dataendtime"]
        missing_values = int(param["param"]["preprocess"]["missingvalues"])
        denoising = int(param["param"]["preprocess"]["denoising"])
        scalerind = int(param["param"]["preprocess"]["scalerind"])
        model = param["param"]["model"]
        # 输入数据
        print("===输入数据===")
        data_input_lst = read_mysql(sqlEngine,ID,col_input,starttime,endtime)
        # 输出数据
        print("===输出数据===")
        data_target_lst = read_mysql(sqlEngine,ID,col_target,starttime,endtime)
        # 数据前处理
        cldata_input_lst = {}
        for i in data_input_lst:
            cldata_input_lst[i] = data_process(data_input_lst[i],missing_values,denoising,scalerind)
        cldata_target_lst = {}
        for i in data_target_lst:
            cldata_target_lst[i] = data_process(data_target_lst[i],missing_values,denoising,scalerind)
        # 模型训练
        print("===模型训练===")
        train_lstm(model["name"],cldata_input_lst['vib'],cldata_target_lst['vib']
                   ,int(model['num']),int(model['tinterval']),int(model['batchsize']),int(model['epochs']))
        print('=======finish=======')
    except Exception as e:
        print ('===error===')
        print (e)
        print (str(e))
        print ('error message:'+e.message)
        raise e
    return True

def saveConfigToFile(data):
    try:
        # 文件夹的命名机组名
        ym = time.strftime('%Y%m')
        calfolder = data["param"]["model"]["name"] + "-" + data["param"]["id"] + "-" + ym

        filename = "config.json"
        filedir = Path('traincalculatins', calfolder)
        filepath = Path('traincalculatins', calfolder, filename)
        filedir.mkdir(parents=True, exist_ok=True)
        with open(filepath, 'w') as json_file:
            json.dump(data, json_file, ensure_ascii=False)
    except Exception as e:
        return False
    else:
        return True

def prelstm():  
    try:

        # 读取信息
        param = request.get_json()
        ID = param["param"]["id"]
        col_input = param["param"]["input"]
        col_target = param["param"]["target"]
        starttime = param["param"]["datastarttime"]
        endtime = param["param"]["dataendtime"]
        missing_values = int(param["param"]["preprocess"]["missingvalues"])
        denoising = int(param["param"]["preprocess"]["denoising"])
        scalerind = int(param["param"]["preprocess"]["scalerind"])
        model = param["param"]["model"]
        # 输入数据
        print("===输入数据===")
        data_input_lst = read_mysql_pre(sqlEngine,ID,col_input,starttime,endtime)
        # 输出数据
        print("===输出数据===")
        data_target_lst = read_mysql_pre(sqlEngine,ID,col_target,starttime,endtime)
        # 数据前处理
        print(data_target_lst)
        print("===前处理===")
        cldata_input_lst = {}
        for i in data_input_lst:
            cldata_input_lst[i] = data_process(data_input_lst[i],missing_values,denoising,scalerind)
        cldata_target_lst = {}
        for i in data_target_lst:
            cldata_target_lst[i] = data_process(data_target_lst[i],missing_values,denoising,scalerind)
        # 模型预测
        print("===预测残差===")
        resid = predict_lstm(model["name"],cldata_input_lst['vib'],cldata_target_lst['vib']
                             ,int(model['num']),int(model['tinterval']))
        resid_table_name = 'id'+ID+'_'+param["name"]
        print("===存入残差,数据表名:%s==="%resid_table_name)
        if not resid.empty:
            write_mysql(mysql_uri,resid_table_name,resid)
        # resid.to_sql('id'+ID+'_'+param["name"],con=sqlEngine, if_exists='append', index=True)
        # bayes
        print("===计算贝叶斯===")
        numb = 1# 单位：小时
        b_pi = Bayes_pi_t(resid,starttime,endtime,numb)
        #存入数据库
        b_pi_table_name = 'id'+ID+'_'+param["name"]+'_bayesresult'
        print("===存入贝叶斯,数据表名:%s==="%b_pi_table_name)
        if not b_pi.empty:
            write_mysql(mysql_uri,b_pi_table_name,b_pi)
        # b_pi.to_sql('id'+ID+'_'+param["name"]+'_bayesresult',con=sqlEngine, if_exists='append', index=True)
        print('=======finish=======')
    except Exception as e:
        print ('===error===')
        print (e)
        print ('error message:'+e.message)
        raise e
    return True

# rest  api（应用执行端口）
@cross_origin()
@application.route('/')
def test():
    print('OK')
    return b'OK '
@cross_origin()
@application.route('/trainlstm', methods=['POST'])
def train():
    executor.submit(trainlstm)
    return b'mainf '
@cross_origin()
@application.route('/prelstm', methods=['POST'])
def prediction():
    executor.submit(prelstm)
    return b'mainf '



if __name__ == '__main__':
    application.run()
