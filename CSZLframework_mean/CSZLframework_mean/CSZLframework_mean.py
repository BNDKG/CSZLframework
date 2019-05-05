#coding=utf-8

import pandas as pd
import numpy as np

import BackTesting
import Strategy
import Display




if __name__ == '__main__':

    

    curstr=Strategy.Strategy()
    dsp2=Display.Display

    bt=BackTesting.BackTesting(curstr,dsp2)

    bt.do()

    dasda=1


