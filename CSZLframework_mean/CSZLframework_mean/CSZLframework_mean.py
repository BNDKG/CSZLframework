#coding=utf-8

import pandas as pd
import numpy as np

import BackTesting
import Strategy
import Display

import FeatureEnvironment
import models

def backtesting():

    curstr=Strategy.Strategy()
    dsp2=Display.Display

    bt=BackTesting.BackTesting(curstr,dsp2)

    bt.do()

    dasda=1


def train():
    #这里加入重新fe选项
    FE_dataset_name=FeatureEnvironment.FE1.create()

    models.LGBmodel.train(FE_dataset_name)

    ddwd=1



if __name__ == '__main__':

    train()
    backtesting()
