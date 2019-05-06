#coding=utf-8

import pandas as pd
import numpy as np

import FeatureEnvironment

import models
import portfolio

class Strategy(object):
    '''
    这里策略需要输入，数据，模型，资产管理方式

    输出交易列表集
    '''
    


    def __init__(self):
        self.PDataSetCreater=FeatureEnvironment.FE1
        self.Pmodel=models.LGBmodel
        self.portfolio=portfolio.portfoliobase

    def Predictall(self):
        getlist=self.Pmodel.predict(self,self.PDataSetCreater.create())

        finallist=self.portfolio.run(getlist)

        return finallist




