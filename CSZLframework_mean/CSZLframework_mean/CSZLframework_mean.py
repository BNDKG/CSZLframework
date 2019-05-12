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
    #选择日期

    dataset_train=Dataget.Dataget.getDataSet('20160101','20190101')
    dataset_test=Dataget.Dataget.getDataSet('20190301','20190510')

    #选择特征工程
    cur_fe=FE.FE1()

    FE_train=cur_fe.create(dataset_train)
    FE_test=cur_fe.create(dataset_test)

    #选择模型
    cur_model=models.LGBmodel()
    #训练模型
    Lgb_model=cur_model.train(FE_train)
    #进行回测
    finalpath=cur_model.predict(FE_test,Lgb_model)
    
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
    cur_model.real_lgb_predict('DataSet20160101to20190101_FE1_LGBmodel.pkl')

    #展示类
    dis=Display.Display()
    dis.show_today()

    asdad=1

if __name__ == '__main__':



    ztry()
    backtesting()
