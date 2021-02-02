# 添加所需要的库
import os
# 添加自己编写的算法
from data_process import data_process
from read_mysql import read_mysql
from lstm_model import train_lstm,predict_lstm,Bayes_pi
# Web应用程序设置
from flask_executor import Executor

import pandas as pd
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
        data_input_lst = read_mysql(sqlEngine,ID,col_input,starttime,endtime)
        #输出数据
        data_target_lst = read_mysql(sqlEngine,ID,col_target,starttime,endtime)
        # 数据前处理
        cldata_input_lst = {}
        for i in data_input_lst:
            cldata_input_lst[i] = data_process(data_input_lst[i],missing_values,denoising,scalerind)
        cldata_target_lst = {}
        for i in data_target_lst:
            cldata_target_lst[i] = data_process(data_target_lst[i],missing_values,denoising,scalerind)
        # 模型训练
        train_lstm(param["name"],cldata_input_lst['vib'],cldata_target_lst['vib']
                   ,int(model['num']),int(model['tinterval']),int(model['batchsize']),int(model['epochs']))
        print('=======finish=======')
    except Exception as e:
        print ('===error===')
        print (e)
        raise e
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
        data_input_lst = read_mysql(sqlEngine,ID,col_input,starttime,endtime)
        #输出数据
        data_target_lst = read_mysql(sqlEngine,ID,col_target,starttime,endtime)
        # 数据前处理
        cldata_input_lst = {}
        for i in data_input_lst:
            cldata_input_lst[i] = data_process(data_input_lst[i],missing_values,denoising,scalerind)
        cldata_target_lst = {}
        for i in data_target_lst:
            cldata_target_lst[i] = data_process(data_target_lst[i],missing_values,denoising,scalerind)
        # 模型预测
        resid = predict_lstm(model["modelname"],cldata_input_lst['vib'],cldata_target_lst['vib']
                             ,int(model['num']),int(model['tinterval']))
        # bayes
        numb = 12
        b_pi = Bayes_pi(resid,numb)
        #存入数据库
        resid.to_sql('id'+ID+'_'+param["name"],con=sqlEngine, if_exists='replace', index=True)
        b_pi.to_sql('id'+ID+'_bayesresult',con=sqlEngine, if_exists='replace', index=True)
        print('=======finish=======')
    except Exception as e:
        print ('===error===')
        print (e)
        raise e
    return True

# rest  api（应用执行端口）
@application.route('/')
def test():
    print('OK')
    return b'OK '
@application.route('/trainlstm', methods=['POST'])
def train():
    executor.submit(trainlstm)
    return b'mainf '
@application.route('/prelstm', methods=['POST'])
def prediction():
    executor.submit(prelstm)
    return b'mainf '



if __name__ == '__main__':

    application.run()

