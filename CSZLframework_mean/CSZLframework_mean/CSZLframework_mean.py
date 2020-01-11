#coding=utf-8

import pandas as pd
import numpy as np

import BackTesting
import Strategy
import Display
from sklearn.preprocessing import LabelEncoder, OneHotEncoder
import FeatureEnvironment as FE
import models
import Dataget
import Display
import sys
import tushare as ts
import datetime
import os
import shutil
import datetime
import time
import random


def backtesting():

    SuperGet=Dataget.Dataget()

    ###刷新资金量
    #SuperGet.update_moneyflow('20160101','20200101')

    ##刷新数据库
    #SuperGet.updatedaily('20160101','20200101')

    ##刷新复权因子
    #SuperGet.updatedaily_adj_factor('20160101','20200101')

    ####刷新经济指标
    ##dataset_adj_train=SuperGet.updatedaily_long_factors('20150101','20191108')



    ##选择日期
    dataset_adj_train=SuperGet.getDataSet_adj_factor('20130101','20160101')
    dataset_adj_test=SuperGet.getDataSet_adj_factor('20160101','20200109')

    dataset_train=SuperGet.getDataSet('20130101','20160101')
    dataset_test=SuperGet.getDataSet('20160101','20200109')

    #测试添加长期指标

    #dataset_long_train=SuperGet.getDataSet_long_factor('20121212','20160620')
    #dataset_long_test=SuperGet.getDataSet_long_factor('20160621','20190921')

    #测试添加资金量指标
    dataset_moneyflow_train=SuperGet.getDataSet_moneyflow('20130101','20160101')
    dataset_moneyflow_test=SuperGet.getDataSet_moneyflow('20160101','20200109')

    #选择特征工程
    #cur_fe=FE.FE3()
    cur_fe=FE.FEg30()
    #cur_fe=FE.FEo30New2()
    #cur_fe=FE.FEn30()
    #cur_fe=FE.FE_h30()
    #cur_fe=FE.FEg30_highstop()

    FE_train=cur_fe.create(dataset_train,dataset_adj_train,dataset_moneyflow_train)
    FE_test=cur_fe.create(dataset_test,dataset_adj_test,dataset_moneyflow_test)

    #选择模型
    cur_model=models.LGBmodel()
    #cur_model=models.LGBmodel_highstop()
    #训练模型
    cur_model_done=cur_model.train(FE_train)
    #进行回测
    finalpath=cur_model.predict(FE_test,cur_model_done)
    
    #展示类
    dis=Display.Display()

    #dis.scatter(finalpath)
    dis.plotall(finalpath)


    sdfsdf=1

def backtesting_forpara():

    SuperGet=Dataget.Dataget()

    ###刷新资金量
    ##SuperGet.update_moneyflow('20190801','20190821')

    ##刷新数据库
    #SuperGet.updatedaily('20190801','20191101')

    ##刷新复权因子
    #SuperGet.updatedaily_adj_factor('20190101','20191101')

    ###刷新经济指标
    ##dataset_adj_train=SuperGet.updatedaily_long_factors('20100101','20190921')



    ##选择日期
    dataset_adj_train=SuperGet.getDataSet_adj_factor('20130101','20160101')
    dataset_adj_test=SuperGet.getDataSet_adj_factor('20160101','20191223')

    dataset_train=SuperGet.getDataSet('20130101','20160101')
    dataset_test=SuperGet.getDataSet('20160101','20191223')

    #测试添加长期指标

    #dataset_long_train=SuperGet.getDataSet_long_factor('20121212','20160620')
    #dataset_long_test=SuperGet.getDataSet_long_factor('20160621','20190921')

    #测试添加资金量指标
    dataset_moneyflow_train=SuperGet.getDataSet_moneyflow('20130101','20160101')
    dataset_moneyflow_test=SuperGet.getDataSet_moneyflow('20160101','20191223')

    #选择特征工程
    cur_fe=FE.FEo30()
    #cur_fe=FE.FEo30New2()

    FE_train=cur_fe.create(dataset_train,dataset_adj_train,dataset_moneyflow_train)
    FE_test=cur_fe.create(dataset_test,dataset_adj_test,dataset_moneyflow_test)

    #选择模型
    cur_model=models.LGBmodel()
    #训练模型
    cur_model_done=cur_model.train(FE_train)
    #进行回测
    finalpath=cur_model.predict(FE_test,cur_model_done)
    
    #展示类
    dis=Display.Display()

    bufferlist=dis.parafirst(finalpath)

    #+++++++++++++++++++++++#

    multpare=[-30,0,0,0,0,0,0,0,0,0,-1]
    #multpare=[-30,0,0,0,0,0,0,0,0,0,0,0,0,0,0,-1]
    cur_base=0
    cur_index=0

    name=['0','1','2','3','4','5','6','7','8','9','Score']
    #name=['0','1','2','3','4','Score']

    zzzz=pd.DataFrame(columns=name)
    nowTime=datetime.datetime.now().strftime("%Y-%m-%d-%H-%M-%S")
    savepathz="para"+nowTime+".csv"
    zzzz.to_csv(savepathz,encoding='gbk')
    ct=0
    changernum=-20

    #Y=[-17,8,-9,0,0,0,0,-1,20,25]
    Y=[-12,-6,-3,-2,-1,1,2,3,6,12]
    #Y=[-18,-14,-11,0,0,0,0,-27,-11,25]
    #Y=[-18,-10,-17,0,0,0,0,-15,-11,25]
    while(1):
        getcsv=pd.read_csv(savepathz,index_col=0,header=0)
        xxx=reproduce(10)
        changernum=reproduce2(changernum)
        multpare[0]=xxx[0]
        multpare[1]=xxx[1]
        multpare[2]=xxx[2]
        multpare[3]=xxx[3]
        multpare[4]=xxx[4]
        multpare[5]=xxx[5]
        multpare[6]=xxx[6]
        multpare[7]=xxx[7]
        multpare[8]=xxx[8]
        multpare[9]=xxx[9]
        #multpare[9]=changernum
        #multpare[10]=35
        #multpare[11]=0
        #multpare[12]=xxx[3]
        #multpare[13]=xxx[4]
        ##multpare[9]=xxx[5]
        #multpare[14]=35
        para=multpare[:10]
        multpare[10]=dis.paramain(bufferlist,para)
        plus_index=getcsv.shape[0]
        getcsv.loc[plus_index]=multpare

        getcsv.to_csv(savepathz,encoding='gbk')
        print(ct)
        ct+=1

    sdfafsdfas=1


def reproduce(counter):
    multpare=list(range(0, counter))

    while(counter):
        multpare[counter-1] = random.randint(-50,50)
        counter-=1
    return multpare
def reproduce2(inputnum):
    inputnum+=1
    return inputnum

def testautoparameter2():

    multpare=[0,0,0,0,0,0,0,0,0,0,-1]
    cur_base=0
    cur_index=0

    name=['0','1','2','3','4','5','6','7','8','9','Score']

    zzzz=pd.DataFrame(columns=name)
    zzzz.to_csv('para.csv',encoding='gbk')

    while(1):
        getcsv=pd.read_csv('para.csv',index_col=0,header=0)
        xxx=reproduce(6)
        multpare[0]=xxx[0]
        multpare[1]=xxx[1]
        multpare[2]=xxx[2]
        multpare[7]=xxx[3]
        multpare[8]=xxx[4]
        multpare[9]=xxx[5]

        multpare[10]=fakebuffer(multpare)
        plus_index=getcsv.shape[0]
        getcsv.loc[plus_index]=multpare

        getcsv.to_csv('para.csv',encoding='gbk')


    print(multpare)

    sadasd=1


def z_back_real_testing():
    #展示类
    dis=Display.Display()

    #dis.scatter(finalpath)
    dis.real_plot_create()

    dis.real_plot_show_plus()

    asfsdf=1

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


    asdad=1
def ztry2():
    REAL_Get=Dataget.Dataget()
    datepath,adjpath=REAL_Get.get_history_dateset()

    REAL_Get.real_get_change(datepath)
    REAL_Get.real_get_adj_change(adjpath)

    #选择特征工程
    cur_fe=FE.FEg30()    
    cur_fe.real_FE()

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


def testofhigh():


    SuperGet=Dataget.Dataget()
    dataset_test=SuperGet.getDataSet('20190101','20190810')


def CSZL_TimeCheck():
    global CurHour
    global CurMinute



    CurHour=int(time.strftime("%H", time.localtime()))
    CurMinute=int(time.strftime("%M", time.localtime()))

    caltemp=CurHour*100+CurMinute

    #return True

    if (caltemp>=1453 and caltemp<=1500):
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



def testoffound():
    #读取token
    f = open('token.txt')
    token = f.read()     #将txt文件的所有内容读入到字符串str中
    f.close()
    pro = ts.pro_api(token)

    #df = pro.fund_nav(end_date='20170101')
    #df = pro.fund_basic(market='O')
    #df.to_csv('foundtest3.csv', encoding='utf_8_sig')


    #读取历史数据防止重复

    df_test2=pd.read_csv('./Database/Found.csv',index_col=0,header=0)
    date_list_old=df_test2['ann_date'].unique().astype(int).astype(str)
    print(date_list_old)
    xxx=1


    date=pro.query('trade_cal', start_date="20130101", end_date="20190901")


    date=date[date["is_open"]==1]
    bufferlist=date["cal_date"]
    print(bufferlist)
    get_list=bufferlist[~bufferlist.isin(date_list_old)].values

    if len(get_list)<2:
        if len(get_list)==1:
            first_date=get_list[0]
            df_all=pro.fund_nav(end_date=first_date)
        else:
            return
    else:

        first_date=get_list[0]
        next_date=get_list[1:]

        df_all=pro.fund_nav(end_date=first_date)

        zcounter=0
        zall=get_list.shape[0]
        for singledate in next_date:
            zcounter+=1
            print(zcounter*100/zall)

            dec=5
            while(dec>0):
                try:
                    time.sleep(1)
                    df = pro.fund_nav(end_date=singledate)

                    df_all=pd.concat([df_all,df])

                    #df_last
                    #print(df_all)
                    break

                except Exception as e:
                    dec-=1
                    time.sleep(5-dec)

            if(dec==0):
                fsefe=1

        #df_all=pd.concat([df_all,df_test])
        #df_all[["trade_date"]]=df_all[["trade_date"]].astype(int)
        #df_all.sort_values("trade_date",inplace=True)

        df_all=df_all.reset_index(drop=True)

        df_all.to_csv('./Database/Found.csv')
    asdfasf=1

def testoffound2():

    df_all=pd.read_csv("./Database/Found.csv",index_col=0,header=0)
    df_all['tomorrow_adj_nav']=df_all.groupby('ts_code')['adj_nav'].shift(-1)
    df_all=df_all[df_all['adj_nav']!=0]
    df_all['pct_chg']=(df_all['tomorrow_adj_nav']-df_all['adj_nav'])/df_all['adj_nav']
    #df_all['groptest']=df_all.groupby('ts_code')['pct_chg'].max()
    df_all.to_csv('./Database/Found2.csv')

    sdfasfsad=1

def testoffound3():

    df_all=pd.read_csv("./Database/Found2.csv",index_col=0,header=0)
    

    test2=df_all.groupby('ts_code')['pct_chg'].sum()
    test2=pd.DataFrame({'rank_sum':test2})
    #test2=test2[test2['rank_sum']>0]

    df_all['goal_rank']=df_all.groupby('ann_date')['pct_chg'].rank(pct=True)

    #150230.SZ 003889.OF 159928.SZ
    test=df_all[df_all['ts_code']=="159919.SZ"]
    test=test[['ann_date','goal_rank']]
    #test=test[['ann_date','goal_rank','pct_chg']]
    df_all.rename(columns={'goal_rank':'my_rank'},inplace=True)

    df_all=pd.merge(df_all, test, how='left', on=['ann_date'])

    df_all['differ_rank']=df_all['my_rank']-df_all['goal_rank']
    df_all['differ_rank']=df_all['differ_rank'].abs()

    test3=df_all.groupby('ts_code')['differ_rank'].sum()
    #test3=pd.DataFrame({'differ_rank_sum':test3})
    test5=df_all.groupby('ts_code')['differ_rank'].count()
    test5=pd.DataFrame({'differ_rank_count':test5,'differ_rank_sum':test3})

    test3=pd.merge(test2, test5, how='left', on=['ts_code'])

    test3['percent_rank']=test3['rank_sum']/(test3['differ_rank_count']+1)*100
    test3['differ_rank_sum']=test3['differ_rank_sum']/(test3['differ_rank_count']+1)*100

    #bad = pd.DataFrame({'bad':bad})
    #regroup =  total.merge(bad,left_index=True,right_index=True, how='left')

    test4=pd.read_csv("./Database/foundforname.csv",index_col=0,header=0)
    test3=pd.merge(test3, test4, how='left', on=['ts_code'])

    test3.to_csv('./Database/test3.csv',encoding="utf_8_sig")
    test2.to_csv('./Database/test2.csv')

    #test.to_csv('./Database/test.csv')
    #print(test)
    df_all.to_csv('./Database/Found3.csv')
    sdfasfsad=1

def testofstop(multbuffer):
    for no in multbuffer:
        if no==0:
            return True

    return False

def fakebuffer(multpare):

    result=input('imputvalue')

    return result

def testautoparameter():

    multpare=[0,0,0,0,0]
    multbuffer=[0,0,0,0,0]
    cur_index=0
    cur_base=0
    flag=1
    while(testofstop(multbuffer)):
        while(1):
            zzz=fakebuffer(multpare)
            if zzz>=cur_base:
                multpare[cur_index]+=flag
            elif flag>0:
                flag=-1
            else:
                break

        multbuffer[cur_index]+=1
        cur_index+=1


    print(multpare)


if __name__ == '__main__':
    
    #ztry2()
    #backtesting_forpara()

    #testautoparameter2()

    #testoffound()
    #testoffound2()
    #testoffound3()


    #df.to_csv('aaaaaa.csv')

    #roundpredit()

    #testofhigh()

    #new_test()
    #z_back_real_testing()
    #selftest()

    #if(sys.argv[1]=='1'):
    #    ztry(1)
    #else:
    #    ztry(0)

    backtesting()
