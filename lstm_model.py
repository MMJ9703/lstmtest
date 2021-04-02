# -*- coding: utf-8 -*-
import os
import numpy as np
import pandas as pd
import math
import tensorflow
import datetime
from tensorflow.keras.models import load_model
from tensorflow.keras import Sequential
from tensorflow.keras.layers import Dense, Dropout, Activation
from tensorflow.keras.layers import LSTM
from tensorflow.keras.callbacks import EarlyStopping
os.environ['TF_CPP_MIN_LOG_LEVEL'] = '3'
# parentDir = os.path.split(os.path.realpath(__file__))[0]#需更改为函数所在路径
# os.chdir(parentDir)
#%%----------------------------------------------------------------------------
def lstm_model(dim,feature_num,outputs_num):#可改
    lstm_model = Sequential()
    lstm_model.add(LSTM(50, input_shape=(dim, feature_num), activation='relu', 
                        kernel_initializer='lecun_uniform', return_sequences=True))
    lstm_model.add(Dropout(0.2))
    lstm_model.add(LSTM(100,return_sequences=False))
    lstm_model.add(Dropout(0.2))
    lstm_model.add(Dense(outputs_num))
    lstm_model.add(Activation("linear"))
    lstm_model.compile(loss='mean_squared_error', optimizer='adam')
    return lstm_model

def lstm_train(name,lstm_model,inputs,targets,BATCH_SIZE,EPOCHS):
    callbacks_list = [tensorflow.keras.callbacks.ModelCheckpoint(
            filepath=name+'.h5',
            monitor='val_loss',
            save_best_only=True),
            tensorflow.keras.callbacks.EarlyStopping(monitor='loss',patience=10)]
    history = lstm_model.fit(inputs,targets,batch_size=BATCH_SIZE,epochs=EPOCHS,callbacks=callbacks_list,validation_split=0.2)
    return history

def lstm_predict(lstm_model,inputs_pre):
    outputs = lstm_model.predict(inputs_pre)
    return outputs
#数据重构,data类型必须为dataframe
def Data_Refactoring(data_input0,data_target0,dim,t):
    '''
    数据重构

    Parameters
    ----------
    data : DataFrame
        数据.
    col_input : list
        输入数据列名.
    col_target : list
        输出数据列名.
    dim : int
        选取前dim个数据对应下一个数据.
    t : int
        每个数据的间隔，最小为1.

    Returns
    -------
    data_input : array
        输入数据.
    data_target : array
        输出数据.

    '''
    data_input = []  # 测试集
    data_input0 = pd.DataFrame(data_input0)
    for i in range(len(data_input0) - dim*t):
        sup = []
        for j in range(dim):
            sup.append(np.array(data_input0.iloc[[i+j*t],:]))
        sup = np.array(sup)
        sup = sup.reshape(sup.shape[0],sup.shape[2])
        data_input.append(sup)
    data_input = np.array(data_input)
    #data_input = np.expand_dims(data_input, axis=2)
    data_target = pd.DataFrame(data_target0)
    data_target = data_target[dim*t:]
    return data_input,data_target
#%%----------------------------------------------------------------------------
def train_lstm(name,x_train_input,x_train_target,num,t_interval,BATCH_SIZE,EPOCHS):
    print("===数据转换===")
    print(x_train_input.index[0])
    print(x_train_input.index[-1])
    x_train_input,x_train_target = Data_Refactoring(x_train_input,x_train_target,num,t_interval)
    print("===lstm训练===")
    lstm_train(name,lstm_model(num,x_train_input.shape[2],x_train_target.shape[1])
               ,x_train_input,x_train_target,BATCH_SIZE,EPOCHS)

def predict_lstm(model_name,x_pre_input,x_pre_target,num,t_interval):
    print(model_name)
    model = load_model(model_name+'.h5')
    x_test_input,x_test_target = Data_Refactoring(x_pre_input,x_pre_target,num,t_interval)
    x_test_pre = lstm_predict(model,x_test_input)
    x_test_pre = pd.DataFrame(x_test_pre,index=x_test_target.index,columns=x_test_target.columns)
    resid = x_test_target-x_test_pre
    return resid

def bayes_hypothesis(resid):
    sub = resid
    x_mean = np.mean(sub)
    x_var = np.std(sub)
    numb = len(sub)
    b_pi = math.sqrt(numb+1)*math.exp(-numb*numb*x_mean**2/(2*(numb+1)*x_var**2))
    #lnb_pi = math.log(b_pi)
    pro = 100*(b_pi/(1+b_pi))
    return b_pi
def Bayes_pi(resid,numb):
    Bayes = []
    n = len(resid)
    # numb = 12
    # i=0
    for i in range(n-numb+1):
        pro_test = bayes_hypothesis(resid.values[i:i+numb])
        Bayes.append(pro_test)
        # print('Bayes confidence\t{:0.3f}'.format(pro_test))
    Bayes = pd.DataFrame(Bayes,index=resid.index.values[numb-1:],columns=['Bayesfactor'])
    Bayes.index.name = 'Time'
    # Bayes.plot()
    return Bayes
def Bayes_pi_t(resid,starttime,endtime,numb):
    Bayes_value = pd.DataFrame(columns=['Bayesfactor'])
    print('数据开始时间:'+str(resid.index[0]))
    print('数据结束时间:'+str(resid.index[-1]))
    if resid.index[-1]-resid.index[0]>2*datetime.timedelta(hours=numb):
        starttime = pd.to_datetime(starttime)-datetime.timedelta(hours=2*numb)
        endtime = pd.to_datetime(endtime)
        dtime = starttime#+datetime.timedelta(minutes=5)
        while dtime+datetime.timedelta(hours=numb) <= endtime:
            # print(dtime)
            if not resid[dtime:dtime+datetime.timedelta(hours=numb)].empty:
                resid_values = resid[dtime:dtime+datetime.timedelta(hours=numb)].values
                pro_test = bayes_hypothesis(resid_values)
                Bayes_value.loc[dtime+datetime.timedelta(hours=numb),'Bayesfactor'] = pro_test
                dtime += datetime.timedelta(minutes=5)
            else:
                dtime += datetime.timedelta(minutes=5)
    Bayes_value.index.name = 'Time'
    return Bayes_value
