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


def backtesting():
    #刷新数据库
    #Dataget.Dataget.updatedaily('20190404','20190510')

    #刷新复权因子
    #Dataget.Dataget.updatedaily_adj_factor('20170101','20190510')
    #选择日期
    dataset_adj_train=Dataget.Dataget.getDataSet_adj_factor('20180101','20190101')


    dataset_train=Dataget.Dataget.getDataSet('20160101','20190101')
    dataset_test=Dataget.Dataget.getDataSet('20150101','20160101')

    #选择特征工程
    cur_fe=FE.FE2()

    FE_train=cur_fe.create(dataset_train,dataset_adj_train)
    FE_test=cur_fe.create(dataset_test)

    #选择模型
    cur_model=models.LGBmodel()
    #训练模型
    cur_model_done=cur_model.train(FE_train)
    #进行回测
    finalpath=cur_model.predict(FE_test,cur_model_done)
    
    #展示类
    dis=Display.Display()

    dis.plotall(finalpath)



def ztry():
    Dataget.Dataget.get_codeanddate_feature()

    Dataget.Dataget.real_get_change()

    #选择特征工程
    cur_fe=FE.FE1()    
    cur_fe.real_FE2()

    #选择模型
    cur_model=models.LGBmodel()
    cur_model.real_lgb_predict('DataSet20100101to20180101_FE1_LGBmodel.pkl')

    #展示类
    dis=Display.Display()
    dis.show_today()

    asdad=1

if __name__ == '__main__':



    #ztry()
    backtesting()
