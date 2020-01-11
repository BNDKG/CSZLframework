#coding=utf-8

import pandas as pd
import numpy as np

import BackTesting
import Strategy
import Display
import FeatureEnvironment as FE
import models
import Dataget
import Display
import sys
import tushare as ts
import datetime
import os
import shutil
import time


def backtesting():

    SuperGet=Dataget.Dataget()
    ##刷新数据库
    #SuperGet.updatedaily('20100101','20190627')

    ##刷新复权因子
    #SuperGet.updatedaily_adj_factor('20100101','20190627')

    ##刷新经济指标
    #dataset_adj_train=SuperGet.updatedaily_long_factors('20100101','20190627')



    ##选择日期
    dataset_adj_train=SuperGet.getDataSet_adj_factor('20120101','20170101')
    dataset_adj_test=SuperGet.getDataSet_adj_factor('20170101','20190727')

    dataset_train=SuperGet.getDataSet('20120101','20170101')
    dataset_test=SuperGet.getDataSet('20170101','20190727')

    #测试添加长期指标

    #dataset_adj_train=Dataget.Dataget.getDataSet_adj_factor('20170101','20180520')
    #dataset_adj_test=Dataget.Dataget.getDataSet_adj_factor('20190101','20190501')

    #dataset_train=Dataget.Dataget.getDataSet('20170101','20180520')
    #dataset_test=Dataget.Dataget.getDataSet('20190101','20190501')

    dataset_long_train=SuperGet.getDataSet_long_factor('20120101','20170101')
    dataset_long_test=SuperGet.getDataSet_long_factor('20170101','20190727')

    #dataset_adj_test=Dataget.Dataget.getDataSet_adj_factor('20100101','20170101')
    #dataset_adj_train=Dataget.Dataget.getDataSet_adj_factor('20170101','20190520')

    #dataset_test=Dataget.Dataget.getDataSet('20100101','20170101')
    #dataset_train=Dataget.Dataget.getDataSet('20170101','20190520')

    #选择特征工程
    cur_fe=FE.FE3()


    FE_train=cur_fe.create(dataset_train,dataset_adj_train,dataset_long_train)
    FE_test=cur_fe.create(dataset_test,dataset_adj_test,dataset_long_test)

    #选择模型
    cur_model=models.LGBmodel()
    #训练模型
    cur_model_done=cur_model.train(FE_train)
    #进行回测
    finalpath=cur_model.predict(FE_test,cur_model_done)
    
    #展示类
    dis=Display.Display()

    #dis.scatter(finalpath)
    dis.plotall(finalpath)

def z_back_real_testing():
    #展示类
    dis=Display.Display()

    #dis.scatter(finalpath)
    dis._remix_csv()

def ztry(day_gap_flag):
    REAL_Get=Dataget.Dataget()
    datepath,adjpath=REAL_Get.get_history_dateset()

    REAL_Get.real_get_change(datepath)
    REAL_Get.real_get_adj_change(adjpath)

    #选择特征工程
    cur_fe=FE.FE3()    
    cur_fe.real_FE(day_gap_flag)

    #
    nowTime=datetime.datetime.now()
    month_sec=nowTime.strftime('%Y%m%d')  

    #选择模型
    cur_model=models.LGBmodel()
    cur_model.real_lgb_predict('lgb1.pkl','out1.csv')
    cur_model.real_lgb_predict('lgb2.pkl','out2.csv')
    cur_model.real_lgb_predict('lgb3.pkl','out3.csv')
    cur_model.real_lgb_predict('lgb4.pkl','out4.csv')

    #展示类
    dis=Display.Display()
    dis.show_today()

    todaypath='./result'+month_sec
    CreateDir(todaypath)

    mycopyfile('out1.csv',todaypath+'/out1.csv')
    mycopyfile('out2.csv',todaypath+'/out2.csv')
    mycopyfile('out3.csv',todaypath+'/out3.csv')
    mycopyfile('out4.csv',todaypath+'/out4.csv')


    time.sleep(10)
    asdad=1
def CreateDir(path):
    isExists=os.path.exists(path)
    # 判断结果
    if not isExists:
        # 如果不存在则创建目录
        os.makedirs(path) 
        print(path+' 目录创建成功')
    else:
        # 如果目录存在则不创建，并提示目录已存在
        print(path+' 目录已存在')

def test1():
    nowTime=datetime.datetime.now()
    month_sec=nowTime.strftime('%Y%m%d')  
    todaypath='./result'+month_sec
    CreateDir(todaypath)

    mycopyfile('out1.csv',todaypath+'/out1.csv')

    dsffse=1


def mycopyfile(srcfile,dstfile):
    if not os.path.isfile(srcfile):
        print ("%s not exist!"%(srcfile))
    else:
        fpath,fname=os.path.split(dstfile)    #分离文件名和路径
        if not os.path.exists(fpath):
            os.makedirs(fpath)                #创建路径
        shutil.copyfile(srcfile,dstfile)      #复制文件
        print ("copy %s -> %s"%( srcfile,dstfile))


def selftest():
    #展示类
    dis=Display.Display()
    dis.show_today()


def new_test():
    df_test=pd.read_csv('fefortest.csv',index_col=0,header=0)

    print(df_test)
            #循环非1的列
    for usecol in df_test.columns.tolist()[1:]:
        if(usecol not in["ts_code","trade_date","tomorrow_chg"] ):
            #将当前列全部转为字符类型
            df_test[usecol] = df_test[usecol].astype('int8')
            
            ##Fit LabelEncoder
            ##对训练集和测试集同时做labelencoder
            #le = LabelEncoder().fit(np.unique(df_test[usecol].unique().tolist()))

            ##At the end 0 will be used for dropped values
            ##同上英文翻译
            #df_test[usecol] = le.transform(df_test[usecol])+1

            #            #以当前关键字从大到小排列相当于每个labelencoder的数字代表了他出现的频繁度(?)
            #            #动原始行前先复制一份出来
            #agg[usecol+'Copy'] = agg[usecol]

            #df_test[usecol] = (pd.merge(df_test[[usecol]], 
            #                          agg[[usecol, usecol+'Copy']], 
            #                          on=usecol, how='left')[usecol+'Copy']
            #                 .replace(np.nan, 0).astype('int').astype('category'))
            df_test=one_hot(df_test,usecol)
            sfdasf=1

    #print(df_test)
    df_test.to_csv('see_onehot')
    asdfaf=1
    #Fit OneHotEncoder

def one_hot(dataset,column_name):

    dummies = pd.get_dummies(dataset[column_name], prefix= column_name,prefix_sep='__')
    dataset.drop([column_name], axis=1, inplace=True)    
    dataset=dataset.join(dummies)
    return dataset

    dsfsfe=1

def CSZL_TimeCheck():
    global CurHour
    global CurMinute



    CurHour=int(time.strftime("%H", time.localtime()))
    CurMinute=int(time.strftime("%M", time.localtime()))

    caltemp=CurHour*100+CurMinute

    #return True

    if (caltemp>=1455 and caltemp<=1500):
        return True
    else:
        return False 

def roundpredit():

    cur_date=datetime.datetime.now().strftime("%Y-%m-%d")
    change_flag=0
    while(True):
        date=datetime.datetime.now()
        day = date.weekday()
        if(day>4):
            time.sleep(10000)
            continue
            dawd=5
        if(day==4):
            cur_inputflag=1
        else:
            cur_inputflag=0

        if(CSZL_TimeCheck()):       

            if(cur_inputflag==1):
                ztry(1)
            else:
                ztry(0)

            time.sleep(10000) 
        

        print(date)
        time.sleep(10)


if __name__ == '__main__':

    roundpredit()

    #test1()
    #new_test()
    #z_back_real_testing()
    #selftest()

    if(sys.argv[1]=='1'):
        ztry(1)
    else:
        ztry(0)

    #backtesting()
