import os
import pandas as pd
import numpy as np
import datetime
from sklearn.impute import SimpleImputer
from sklearn import preprocessing
from DWPB_denoising import DWPB

def pre_process(data_1):

    # 检查是否包含无穷数据, 去除非数值列
    Flo_data = data_1.loc[:, data_1.dtypes == 'float64']
    train_inf = np.isinf(Flo_data)
    Flo_data[train_inf] = np.nan

    ###删除空值>80%的列
    thresh_len = Flo_data.shape[0]
    col_data= Flo_data.dropna(axis=1,how='all',thresh=0.8*thresh_len)
    col_data=col_data.loc[:,col_data.dtypes== 'float64']

    #空值中位数填充
    imp_median = SimpleImputer(missing_values=np.nan,strategy='median')
    fill_data = imp_median.fit_transform(col_data.values)
    #fill_data=drop_data.fillna(value=None,method='bfill',axis=0,limit=None)
    fill_data=pd.DataFrame(fill_data,index=col_data.index, columns=col_data.columns)

    return fill_data

def normalize_X(X, scalerInd = 1):
    """
    Normalize the given data frame to a standardized zero mean and deviation
    
    X = data frame or arrary
    normScaler = 1: StandardScaler, otherwise, minMaxScaler

    """
    
    # replace NaN with 0
    X = X.replace(np.inf, np.nan)
    X = X.replace(-np.inf, np.nan)
    X = X.replace('nan', np.nan)
    X.fillna(0,inplace=True)       
    
    #xvalue = X.values.reshape(-1, 1) #X.values 
    X = pd.DataFrame(X)
    # print(X.columns)
    xvalue = X.values
        
    if scalerInd ==1:
        normX_scaler = preprocessing.StandardScaler().fit(xvalue)             
    elif scalerInd ==0:
        normX_scaler = preprocessing.MinMaxScaler().fit(xvalue) 
               
    xScaled = normX_scaler.transform(xvalue)
    normX = pd.DataFrame(xScaled)
    
    normX.index = X.index
    normX.columns = X.columns.values
    return normX

def data_process(data,missing_values,denoising,scalerind):
    # 缺失值处理
    print('缺失值处理')
    if missing_values == 1:
        inputdata_1 = data
        outputdata_1 = pre_process(inputdata_1)
    else:
        outputdata_1 = inputdata_1
    # 数据去噪
    print('数据去噪')
    if denoising == 1:
        inputdata_2 = outputdata_1
        outputdata_2 = DWPB('db8',3).denoising_process(inputdata_2)
        outputdata_2.columns = inputdata_2.columns.values
    else:
        outputdata_2 = inputdata_2
    # 数据标准化/归一化
    print('归一化')
    if scalerind == 1:
        inputdata_3 = outputdata_2
        outputdata_3 = normalize_X(inputdata_3, scalerInd = 1)
    elif scalerind == 0:
        inputdata_3 = outputdata_2
        outputdata_3 = normalize_X(inputdata_3, scalerInd = 0)
    else:
        outputdata_3 = inputdata_3
    
    result = outputdata_3
    return result
