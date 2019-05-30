#coding=utf-8
import pandas as pd
import numpy as np
import matplotlib
import matplotlib.pyplot as plt

from numpy import *

plt.ioff()
#matplotlib.use('TkAgg')
#plt.ion()

class Display(object):
    """description of class"""

    def __init__(self):
        pass

    def scatter(self,path):
        showsource=pd.read_csv(path,index_col=0,header=0)
        databuffer=showsource['trade_date'].unique()

        for curdata in databuffer:

            cur_show=showsource[showsource["trade_date"]==curdata]

            b=cur_show.sort_values(by="mix" , ascending=False) 

            x_axis=range(len(b))
            y_axis=b['tomorrow_chg']

            self.show(x_axis,y_axis,title=curdata)

            adwda=1

    def show(self,x_axis,y_axis,x_label="xlabel",y_label="ylabel ",title="title",x_tick="",y_tick="",colori="blue"):
            plt.figure(figsize=(19, 11))
            plt.scatter(x_axis, y_axis,s=8)
            #plt.xlim(30, 160)
            #plt.ylim(5, 50)
            #plt.axis()
    
            plt.title(title,color=colori)
            plt.xlabel("rank")
            plt.ylabel("chg_pct")

            if(x_tick!=""or y_tick!=""):
                plt.xticks(x_axis,x_tick)
                plt.yticks(y_axis,y_tick)

            #plt.pause(2)
            plt.show()

    def plotall(self,path):

        #days,show1,show2=get_allchange()


        #get_codeanddate_feature()

        #feature_env_codeanddate3('2017')

        #lgb_train_2('2017')

        days,show3=self.show_all_rate(path)

        #plt.plot(days,show1,c='blue',label="000001")
        #plt.plot(days,show2,c='red',label="399006")
        plt.plot(days,show3,c='green',label="my model head10mean")

        plt.legend()

        plt.show()

        input()

    def show_all_rate(self,path):
        showsource=pd.read_csv(path,index_col=0,header=0)
        databuffer=showsource['trade_date'].unique()

        showsource['mix']=showsource['0']*(-8)+showsource['1']*(-8)+showsource['2']*(-3)+showsource['3']*(-2)+showsource['4']*(-1)+showsource['5']*1+showsource['6']*2+showsource['7']*3+showsource['8']*7+showsource['9']*12
        #multlist=[-12,-5,-3,-2,-1.5,-1,-0.75,-0.5,-0.25,0,0,0.25,0.5,0.75,1,1.5,2,3,5,12]
        

        changer=[]
        for curdata in databuffer:

            cur_show=showsource[showsource["trade_date"]==curdata]
            b=cur_show.sort_values(by="mix" , ascending=False)
            #b=cur_show.sort_values(by="9" , ascending=True)
            #d=b.head(10)
            #e=d.sort_values(by="mix" , ascending=True)
        

            #b=cur_show[cur_show['mix']>0.40]
            average=b.head(1)['tomorrow_chg'].mean()
            changer.append(average)

            adwda=1


        days2,show=self.standard_show(changer,day_interval=1)

        return days2,show

    def standard_show(self,changer,first_base_income=100000,day_interval=2,label="自己"):
    
        start_from=first_base_income
        show=[]
        for curchange in changer:
            start_from=start_from+(first_base_income/100/day_interval)*curchange
            show.append(start_from)

        #print(show)
        len_show=len(show)
        days=np.arange(1,len_show+1)

        fig=plt.figure(figsize=(6,3))


        #plt.show()

        return days,show

    def show_today(self):

        show=pd.read_csv('todaypredict.csv',index_col=0,header=0)
        datamax=show['trade_date'].max()
        #datamax=20190408

        show=show[show['trade_date']==datamax]

        show=show[['ts_code','0','9','mix']]

        #ascending表示升降序
        b=show.sort_values(by="mix" , ascending=False) 
        c=show.sort_values(by="9" , ascending=False) 
        final_mix=b.head(20)
        final_9=c.head(20)

        arr=[600461,603389,300384]
        final_have=show[show['ts_code'].isin(arr)]

        pd.set_option('display.max_columns', None)
        print('综合成绩')
        print(final_mix)
        print('极限成绩')
        print(final_9)
        print('当前拥有')
        print(final_have)

        fsfef=1