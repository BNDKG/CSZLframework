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

def backtesting():

    SuperGet=Dataget.Dataget()
    ##刷新数据库
    #SuperGet.updatedaily('20100101','20190524')

    ##刷新复权因子
    #SuperGet.updatedaily_adj_factor('20100101','20190530')

    #刷新经济指标
    #dataset_adj_train=Dataget.Dataget.updatedaily_long_factors('20100101','20190520')



    ##选择日期
    dataset_adj_train=SuperGet.getDataSet_adj_factor('20100101','20190520')
    dataset_adj_test=SuperGet.getDataSet_adj_factor('20170101','20190529')

    dataset_train=SuperGet.getDataSet('20100101','20190520')
    dataset_test=SuperGet.getDataSet('20170101','20190529')

    #测试添加长期指标

    #dataset_adj_train=Dataget.Dataget.getDataSet_adj_factor('20170101','20180520')
    #dataset_adj_test=Dataget.Dataget.getDataSet_adj_factor('20190101','20190501')

    #dataset_train=Dataget.Dataget.getDataSet('20170101','20180520')
    #dataset_test=Dataget.Dataget.getDataSet('20190101','20190501')

    dataset_long_train=SuperGet.getDataSet_long_factor('20100101','20190520')
    dataset_long_test=SuperGet.getDataSet_long_factor('20170101','20190529')

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



def ztry(day_gap_flag):
    REAL_Get=Dataget.Dataget()
    datepath,adjpath=REAL_Get.get_history_dateset()

    #REAL_Get.real_get_change(datepath)
    REAL_Get.real_get_adj_change(adjpath)

    #选择特征工程
    cur_fe=FE.FE3()    
    cur_fe.real_FE(day_gap_flag)

    #选择模型
    cur_model=models.LGBmodel()
    cur_model.real_lgb_predict('DataSet20170101to20190520_FE3_LGBmodel.pkl')

    #展示类
    dis=Display.Display()
    dis.show_today()

    asdad=1

if __name__ == '__main__':

    #if(sys.argv[1]=='1'):
    #    ztry(1)
    #else:
    #    ztry(0)

    backtesting()
