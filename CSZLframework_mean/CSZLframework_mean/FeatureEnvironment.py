#coding=utf-8
import pandas as pd
import numpy as np
import sys
import os

import datetime

class FEbase(object):
    """description of class"""
    def __init__(self, **kwargs):
        pass


    def create(self,*DataSetName):
        #print (self.__class__.__name__)
        (filepath, tempfilename) = os.path.split(DataSetName[0])
        (filename, extension) = os.path.splitext(tempfilename)

        #bufferstring='savetest2017.csv'
        bufferstringoutput=filepath+'/'+filename+'_'+self.__class__.__name__+extension

        if(os.path.exists(bufferstringoutput)==False):    

            #df_all=pd.read_csv(bufferstring,index_col=0,header=0,nrows=100000)
            df_all=self.core(DataSetName)
            df_all.to_csv(bufferstringoutput)

        return bufferstringoutput

    def core(self,df_all,Data_adj_name=''):
        return df_all

    def real_FE():
        return 0

class FE1(FEbase):
    def __init__(self):
        pass

    def core(self,DataSetName):

        df_all=pd.read_csv(DataSetName[0],index_col=0,header=0)
        df_all.drop(['change','vol'],axis=1,inplace=True)
    
        #明日幅度
        #tm1=df_all.groupby('ts_code')['pct_chg'].shift(-1)
        #tm2=df_all.groupby('ts_code')['pct_chg'].shift(-2)
        #tm3=df_all.groupby('ts_code')['pct_chg'].shift(-3)
        #df_all['tomorrow_chg']=((100+tm1)*(100+tm2)*(100+tm3)-1000000)/10000
        df_all['tomorrow_chg']=df_all.groupby('ts_code')['pct_chg'].shift(-1)

        df_all['tomorrow_chg_rank']=df_all.groupby('trade_date')['tomorrow_chg'].rank(pct=True)
        df_all['tomorrow_chg_rank']=df_all['tomorrow_chg_rank']*9.9//1
        #是否停
        df_all['high_stop']=0
        df_all.loc[df_all['pct_chg']>9,'high_stop']=1
        df_all.loc[(df_all['pct_chg']<5.5) & (4.5<df_all['pct_chg']),'high_stop']=1


        #真实价格范围
        df_all['price_real_rank']=df_all.groupby('trade_date')['pre_close'].rank(pct=True)
        df_all['price_real_rank']=df_all['price_real_rank']*10//1
        #1日
        df_all['chg_rank']=df_all.groupby('trade_date')['pct_chg'].rank(pct=True)
        df_all['chg_rank']=df_all['chg_rank']*10//1


        #6日
        xxx=df_all.groupby('ts_code')['chg_rank'].rolling(6).sum().reset_index()
        xxx.set_index(['level_1'], drop=True, append=False, inplace=True, verify_integrity=False)
        xxx.drop(['ts_code'],axis=1,inplace=True)

        df_all=df_all.join(xxx, lsuffix='', rsuffix='_6')

        df_all['chg_rank_6']=df_all.groupby('trade_date')['chg_rank_6'].rank(pct=True)
        df_all['chg_rank_6']=df_all['chg_rank_6']*10//1

        #10日
        xxx=df_all.groupby('ts_code')['chg_rank'].rolling(10).sum().reset_index()
        xxx.set_index(['level_1'], drop=True, append=False, inplace=True, verify_integrity=False)
        xxx.drop(['ts_code'],axis=1,inplace=True)

        df_all=df_all.join(xxx, lsuffix='', rsuffix='_10')

        df_all['chg_rank_10']=df_all.groupby('trade_date')['chg_rank_10'].rank(pct=True)
        df_all['chg_rank_10']=df_all['chg_rank_10']*10//1

        #3日
        xxx=df_all.groupby('ts_code')['chg_rank'].rolling(3).sum().reset_index()
        xxx.set_index(['level_1'], drop=True, append=False, inplace=True, verify_integrity=False)
        xxx.drop(['ts_code'],axis=1,inplace=True)

        df_all=df_all.join(xxx, lsuffix='', rsuffix='_3')

        df_all['chg_rank_3']=df_all.groupby('trade_date')['chg_rank_3'].rank(pct=True)
        df_all['chg_rank_3']=df_all['chg_rank_3']*10//1

        #print(df_all)

        #10日均量
        xxx=df_all.groupby('ts_code')['amount'].rolling(10).mean().reset_index()
        xxx.set_index(['level_1'], drop=True, append=False, inplace=True, verify_integrity=False)
        xxx.drop(['ts_code'],axis=1,inplace=True)
        df_all=df_all.join(xxx, lsuffix='_1', rsuffix='_10')

        #当日量占比
        df_all['pst_amount']=df_all['amount_1']/df_all['amount_10']
        df_all.drop(['amount_1','amount_10'],axis=1,inplace=True)
        #当日量排名
        df_all['pst_amount_rank']=df_all.groupby('trade_date')['pst_amount'].rank(pct=True)
        df_all['pst_amount_rank']=df_all['pst_amount_rank']*10//1

        #计算三种比例rank
        dolist=['open','high','low']

        for curc in dolist:
            buffer=((df_all[curc]-df_all['pre_close'])*100)/df_all['close']
            df_all[curc]=buffer
            df_all[curc]=df_all.groupby('trade_date')[curc].rank(pct=True)
            df_all[curc]=df_all[curc]*10//1

        #加入昨日rank
        df_all['yesterday_open']=df_all.groupby('ts_code')['open'].shift(1)
        df_all['yesterday_high']=df_all.groupby('ts_code')['high'].shift(1)
        df_all['yesterday_low']=df_all.groupby('ts_code')['low'].shift(1)
        df_all['yesterday_pst_amount_rank']=df_all.groupby('ts_code')['pst_amount_rank'].shift(1)
        #加入前日open
        df_all['yesterday2_open']=df_all.groupby('ts_code')['open'].shift(2)

        df_all.drop(['close','pre_close','pct_chg','pst_amount'],axis=1,inplace=True)
        #暂时不用的列
        df_all=df_all[df_all['high_stop']==0]
        #'tomorrow_chg'
        df_all.drop(['high_stop'],axis=1,inplace=True)



        df_all.dropna(axis=0,how='any',inplace=True)

        print(df_all)
        df_all=df_all.reset_index(drop=True)

        return df_all

    def real_FE2(self):

        bufferstring='real_now.csv'

        df_all=pd.read_csv(bufferstring,index_col=0,header=0)
        #df_all=pd.read_csv(bufferstring,index_col=0,header=0,nrows=100000)
    
        #df_all.drop(['change','vol'],axis=1,inplace=True)
    

        #明日幅度
        #tm1=df_all.groupby('ts_code')['pct_chg'].shift(-1)
        #tm2=df_all.groupby('ts_code')['pct_chg'].shift(-2)
        #tm3=df_all.groupby('ts_code')['pct_chg'].shift(-3)
        #df_all['tomorrow_chg']=((100+tm1)*(100+tm2)*(100+tm3)-1000000)/10000
        #df_all['tomorrow_chg']=df_all.groupby('ts_code')['pct_chg'].shift(-1)

        #df_all['tomorrow_chg_rank']=df_all.groupby('trade_date')['tomorrow_chg'].rank(pct=True)
        #df_all['tomorrow_chg_rank']=df_all['tomorrow_chg_rank']*9.9//1
        #是否停
        df_all['high_stop']=0
        df_all.loc[df_all['pct_chg']>9,'high_stop']=1
        df_all.loc[(df_all['pct_chg']<5.5) & (4.5<df_all['pct_chg']),'high_stop']=1


        #真实价格范围
        df_all['price_real_rank']=df_all.groupby('trade_date')['pre_close'].rank(pct=True)
        df_all['price_real_rank']=df_all['price_real_rank']*10//1
        #1日
        df_all['chg_rank']=df_all.groupby('trade_date')['pct_chg'].rank(pct=True)
        df_all['chg_rank']=df_all['chg_rank']*10//1


        #6日
        xxx=df_all.groupby('ts_code')['chg_rank'].rolling(6).sum().reset_index()
        xxx.set_index(['level_1'], drop=True, append=False, inplace=True, verify_integrity=False)
        xxx.drop(['ts_code'],axis=1,inplace=True)

        df_all=df_all.join(xxx, lsuffix='', rsuffix='_6')

        df_all['chg_rank_6']=df_all.groupby('trade_date')['chg_rank_6'].rank(pct=True)
        df_all['chg_rank_6']=df_all['chg_rank_6']*10//1

        #10日
        xxx=df_all.groupby('ts_code')['chg_rank'].rolling(10).sum().reset_index()
        xxx.set_index(['level_1'], drop=True, append=False, inplace=True, verify_integrity=False)
        xxx.drop(['ts_code'],axis=1,inplace=True)

        df_all=df_all.join(xxx, lsuffix='', rsuffix='_10')

        df_all['chg_rank_10']=df_all.groupby('trade_date')['chg_rank_10'].rank(pct=True)
        df_all['chg_rank_10']=df_all['chg_rank_10']*10//1

        #3日
        xxx=df_all.groupby('ts_code')['chg_rank'].rolling(3).sum().reset_index()
        xxx.set_index(['level_1'], drop=True, append=False, inplace=True, verify_integrity=False)
        xxx.drop(['ts_code'],axis=1,inplace=True)

        df_all=df_all.join(xxx, lsuffix='', rsuffix='_3')

        df_all['chg_rank_3']=df_all.groupby('trade_date')['chg_rank_3'].rank(pct=True)
        df_all['chg_rank_3']=df_all['chg_rank_3']*10//1

        #print(df_all)

        #10日均量
        xxx=df_all.groupby('ts_code')['amount'].rolling(10).mean().reset_index()
        xxx.set_index(['level_1'], drop=True, append=False, inplace=True, verify_integrity=False)
        xxx.drop(['ts_code'],axis=1,inplace=True)
        df_all=df_all.join(xxx, lsuffix='_1', rsuffix='_10')

        #当日量占比
        df_all['pst_amount']=df_all['amount_1']/df_all['amount_10']
        df_all.drop(['amount_1','amount_10'],axis=1,inplace=True)
        #当日量排名
        df_all['pst_amount_rank']=df_all.groupby('trade_date')['pst_amount'].rank(pct=True)
        df_all['pst_amount_rank']=df_all['pst_amount_rank']*10//1

        #计算三种比例rank
        dolist=['open','high','low']

        for curc in dolist:
            buffer=((df_all[curc]-df_all['pre_close'])*100)/df_all['pre_close']
            df_all[curc]=buffer
            df_all[curc]=df_all.groupby('trade_date')[curc].rank(pct=True)
            df_all[curc]=df_all[curc]*10//1

        #加入昨日rank
        df_all['yesterday_open']=df_all.groupby('ts_code')['open'].shift(1)
        df_all['yesterday_high']=df_all.groupby('ts_code')['high'].shift(1)
        df_all['yesterday_low']=df_all.groupby('ts_code')['low'].shift(1)
        df_all['yesterday_pst_amount_rank']=df_all.groupby('ts_code')['pst_amount_rank'].shift(1)
        #加入前日open
        df_all['yesterday2_open']=df_all.groupby('ts_code')['open'].shift(2)

        df_all.drop(['close','pre_close','pct_chg','pst_amount'],axis=1,inplace=True)
        #暂时不用的列
        df_all=df_all[df_all['high_stop']==0]
        #'tomorrow_chg'
        df_all.drop(['high_stop'],axis=1,inplace=True)



        df_all.dropna(axis=0,how='any',inplace=True)

        print(df_all)
        df_all=df_all.reset_index(drop=True)

        df_all.to_csv('today_train.csv')
        dwdw=1

class FE2(FEbase):
    def __init__(self):
        pass

    def core(self,DataSetName):

        df_data=pd.read_csv(DataSetName[0],index_col=0,header=0)
        df_adj_all=pd.read_csv(DataSetName[1],index_col=0,header=0)

        df_all=pd.merge(df_data, df_adj_all, how='inner', on=['ts_code','trade_date'])

        #===================================================================================================================================#

        #加入gap_day特征
        start=df_all['trade_date'].apply(str)[0]
        end=df_all['trade_date'].apply(str)[df_all.shape[0]-1]
        xxx=pd.date_range(start,end)

        df = pd.DataFrame(xxx)
        df.columns = ['trade_date']
        df['trade_date']=df['trade_date'].map(str).map(lambda x : x[:4]+x[5:7]+x[8:10]).astype("int64")

        yyy=df_all['trade_date']
        zzz2=yyy.unique()
        df_2=pd.DataFrame(zzz2)
        df_2.columns = ['trade_date']
        df_2['day_flag']=1
    
        result = pd.merge(df, df_2, how='left', on=['trade_date'])
        result['day_flag2']=result['day_flag'].shift(-1)
        result['gap_day']=0

        result.loc[(result['day_flag']==1) & (result['day_flag2']!=1),'gap_day']=1

        result.drop(['day_flag','day_flag2'],axis=1,inplace=True)

        df_all=pd.merge(df_all, result, how='left', on=['trade_date'])

        #===================================================================================================================================#

        ##复权后价格
        df_all['real_price']=df_all['close']*df_all['adj_factor']
        
        ##30日最低比值
        #xxx=df_all.groupby('ts_code')['real_price'].rolling(30).min().reset_index()
        #xxx.set_index(['level_1'], drop=True, append=False, inplace=True, verify_integrity=False)
        #xxx.drop(['ts_code'],axis=1,inplace=True)
        
        #df_all=df_all.join(xxx, lsuffix='', rsuffix='_30min')
        ##bbb=df_all.groupby('ts_code')['real_price'].agg({'all_min':np.min, 'all_max': np.max}).reset_index()
        ##ccc=pd.merge(df_all, bbb, how='inner', on=['ts_code'])
        #df_all['30_pct']=(df_all['real_price']-df_all['real_price_30min'])/df_all['real_price_30min']
        #df_all['30_pct_rank']=df_all.groupby('trade_date')['30_pct'].rank(pct=True)
        #df_all['30_pct_rank']=df_all['30_pct_rank']*10//1

        ##30日最高比值
        #xxx=df_all.groupby('ts_code')['real_price'].rolling(30).max().reset_index()
        #xxx.set_index(['level_1'], drop=True, append=False, inplace=True, verify_integrity=False)
        #xxx.drop(['ts_code'],axis=1,inplace=True)
        
        #df_all=df_all.join(xxx, lsuffix='', rsuffix='_30max')
        ##bbb=df_all.groupby('ts_code')['real_price'].agg({'all_min':np.min, 'all_max': np.max}).reset_index()
        ##ccc=pd.merge(df_all, bbb, how='inner', on=['ts_code'])
        #df_all['30_pct_max']=(df_all['real_price']-df_all['real_price_30max'])/df_all['real_price_30max']
        #df_all['30_pct_max_rank']=df_all.groupby('trade_date')['30_pct_max'].rank(pct=True)
        #df_all['30_pct_max_rank']=df_all['30_pct_max_rank']*10//1

        #df_all.drop(['30_pct','real_price_30min','30_pct_max','real_price_30max'],axis=1,inplace=True)

        df_all.drop(['change','vol'],axis=1,inplace=True)

        #===================================================================================================================================#
    
        #明日幅度
        #tm1=df_all.groupby('ts_code')['pct_chg'].shift(-1)
        #tm2=df_all.groupby('ts_code')['pct_chg'].shift(-2)
        #tm3=df_all.groupby('ts_code')['pct_chg'].shift(-3)
        #df_all['tomorrow_chg']=((100+tm1)*(100+tm2)*(100+tm3)-1000000)/10000
        df_all['tomorrow_chg']=df_all.groupby('ts_code')['pct_chg'].shift(-1)
        #明日排名
        df_all['tomorrow_chg_rank']=df_all.groupby('trade_date')['tomorrow_chg'].rank(pct=True)
        df_all['tomorrow_chg_rank']=df_all['tomorrow_chg_rank']*9.9//1
        #是否停
        df_all['high_stop']=0
        df_all.loc[df_all['pct_chg']>9,'high_stop']=1
        df_all.loc[(df_all['pct_chg']<5.4) & (4.6<df_all['pct_chg']),'high_stop']=1


        #真实价格范围(区分实际股价高低)
        df_all['price_real_rank']=df_all.groupby('trade_date')['pre_close'].rank(pct=True)
        df_all['price_real_rank']=df_all['price_real_rank']*10//1
        #1日
        df_all['chg_rank']=df_all.groupby('trade_date')['pct_chg'].rank(pct=True)
        df_all['chg_rank']=df_all['chg_rank']*10//1


        #6日
        xxx=df_all.groupby('ts_code')['chg_rank'].rolling(6).sum().reset_index()
        xxx.set_index(['level_1'], drop=True, append=False, inplace=True, verify_integrity=False)
        xxx.drop(['ts_code'],axis=1,inplace=True)

        df_all=df_all.join(xxx, lsuffix='', rsuffix='_6')

        df_all['chg_rank_6']=df_all.groupby('trade_date')['chg_rank_6'].rank(pct=True)
        df_all['chg_rank_6']=df_all['chg_rank_6']*10//1

        #10日
        xxx=df_all.groupby('ts_code')['chg_rank'].rolling(10).sum().reset_index()
        xxx.set_index(['level_1'], drop=True, append=False, inplace=True, verify_integrity=False)
        xxx.drop(['ts_code'],axis=1,inplace=True)

        df_all=df_all.join(xxx, lsuffix='', rsuffix='_10')

        df_all['chg_rank_10']=df_all.groupby('trade_date')['chg_rank_10'].rank(pct=True)
        df_all['chg_rank_10']=df_all['chg_rank_10']*10//1

        #3日
        xxx=df_all.groupby('ts_code')['chg_rank'].rolling(3).sum().reset_index()
        xxx.set_index(['level_1'], drop=True, append=False, inplace=True, verify_integrity=False)
        xxx.drop(['ts_code'],axis=1,inplace=True)

        df_all=df_all.join(xxx, lsuffix='', rsuffix='_3')

        df_all['chg_rank_3']=df_all.groupby('trade_date')['chg_rank_3'].rank(pct=True)
        df_all['chg_rank_3']=df_all['chg_rank_3']*10//1

        #print(df_all)

        #10日均量
        xxx=df_all.groupby('ts_code')['amount'].rolling(10).mean().reset_index()
        xxx.set_index(['level_1'], drop=True, append=False, inplace=True, verify_integrity=False)
        xxx.drop(['ts_code'],axis=1,inplace=True)
        df_all=df_all.join(xxx, lsuffix='_1', rsuffix='_10')

        #当日量占比
        df_all['pst_amount']=df_all['amount_1']/df_all['amount_10']
        df_all.drop(['amount_1','amount_10'],axis=1,inplace=True)
        #当日量排名
        df_all['pst_amount_rank']=df_all.groupby('trade_date')['pst_amount'].rank(pct=True)
        df_all['pst_amount_rank']=df_all['pst_amount_rank']*10//1

        #计算三种比例rank
        dolist=['open','high','low']

        for curc in dolist:
            buffer=((df_all[curc]-df_all['pre_close'])*100)/df_all['close']
            df_all[curc]=buffer
            df_all[curc]=df_all.groupby('trade_date')[curc].rank(pct=True)
            df_all[curc]=df_all[curc]*10//1

        #加入昨日rank
        df_all['yesterday_open']=df_all.groupby('ts_code')['open'].shift(1)
        df_all['yesterday_high']=df_all.groupby('ts_code')['high'].shift(1)
        df_all['yesterday_low']=df_all.groupby('ts_code')['low'].shift(1)
        df_all['yesterday_pst_amount_rank']=df_all.groupby('ts_code')['pst_amount_rank'].shift(1)
        #加入前日open
        df_all['yesterday2_open']=df_all.groupby('ts_code')['open'].shift(2)

        df_all.drop(['close','pre_close','pct_chg','pst_amount','adj_factor','real_price'],axis=1,inplace=True)
        #暂时不用的列
        df_all=df_all[df_all['high_stop']==0]
        #'tomorrow_chg'
        df_all.drop(['high_stop'],axis=1,inplace=True) 

        df_all.dropna(axis=0,how='any',inplace=True)

        print(df_all)
        df_all=df_all.reset_index(drop=True)

        return df_all

    def real_FE2(self):

        bufferstring='real_now.csv'

        df_all=pd.read_csv(bufferstring,index_col=0,header=0)
        #df_all=pd.read_csv(bufferstring,index_col=0,header=0,nrows=100000)
    
        #df_all.drop(['change','vol'],axis=1,inplace=True)
    

        #明日幅度
        #tm1=df_all.groupby('ts_code')['pct_chg'].shift(-1)
        #tm2=df_all.groupby('ts_code')['pct_chg'].shift(-2)
        #tm3=df_all.groupby('ts_code')['pct_chg'].shift(-3)
        #df_all['tomorrow_chg']=((100+tm1)*(100+tm2)*(100+tm3)-1000000)/10000
        #df_all['tomorrow_chg']=df_all.groupby('ts_code')['pct_chg'].shift(-1)

        #df_all['tomorrow_chg_rank']=df_all.groupby('trade_date')['tomorrow_chg'].rank(pct=True)
        #df_all['tomorrow_chg_rank']=df_all['tomorrow_chg_rank']*9.9//1
        #是否停
        df_all['high_stop']=0
        df_all.loc[df_all['pct_chg']>9,'high_stop']=1
        df_all.loc[(df_all['pct_chg']<5.5) & (4.5<df_all['pct_chg']),'high_stop']=1


        #真实价格范围
        df_all['price_real_rank']=df_all.groupby('trade_date')['pre_close'].rank(pct=True)
        df_all['price_real_rank']=df_all['price_real_rank']*10//1
        #1日
        df_all['chg_rank']=df_all.groupby('trade_date')['pct_chg'].rank(pct=True)
        df_all['chg_rank']=df_all['chg_rank']*10//1


        #6日
        xxx=df_all.groupby('ts_code')['chg_rank'].rolling(6).sum().reset_index()
        xxx.set_index(['level_1'], drop=True, append=False, inplace=True, verify_integrity=False)
        xxx.drop(['ts_code'],axis=1,inplace=True)

        df_all=df_all.join(xxx, lsuffix='', rsuffix='_6')

        df_all['chg_rank_6']=df_all.groupby('trade_date')['chg_rank_6'].rank(pct=True)
        df_all['chg_rank_6']=df_all['chg_rank_6']*10//1

        #10日
        xxx=df_all.groupby('ts_code')['chg_rank'].rolling(10).sum().reset_index()
        xxx.set_index(['level_1'], drop=True, append=False, inplace=True, verify_integrity=False)
        xxx.drop(['ts_code'],axis=1,inplace=True)

        df_all=df_all.join(xxx, lsuffix='', rsuffix='_10')

        df_all['chg_rank_10']=df_all.groupby('trade_date')['chg_rank_10'].rank(pct=True)
        df_all['chg_rank_10']=df_all['chg_rank_10']*10//1

        #3日
        xxx=df_all.groupby('ts_code')['chg_rank'].rolling(3).sum().reset_index()
        xxx.set_index(['level_1'], drop=True, append=False, inplace=True, verify_integrity=False)
        xxx.drop(['ts_code'],axis=1,inplace=True)

        df_all=df_all.join(xxx, lsuffix='', rsuffix='_3')

        df_all['chg_rank_3']=df_all.groupby('trade_date')['chg_rank_3'].rank(pct=True)
        df_all['chg_rank_3']=df_all['chg_rank_3']*10//1

        #print(df_all)

        #10日均量
        xxx=df_all.groupby('ts_code')['amount'].rolling(10).mean().reset_index()
        xxx.set_index(['level_1'], drop=True, append=False, inplace=True, verify_integrity=False)
        xxx.drop(['ts_code'],axis=1,inplace=True)
        df_all=df_all.join(xxx, lsuffix='_1', rsuffix='_10')

        #当日量占比
        df_all['pst_amount']=df_all['amount_1']/df_all['amount_10']
        df_all.drop(['amount_1','amount_10'],axis=1,inplace=True)
        #当日量排名
        df_all['pst_amount_rank']=df_all.groupby('trade_date')['pst_amount'].rank(pct=True)
        df_all['pst_amount_rank']=df_all['pst_amount_rank']*10//1

        #计算三种比例rank
        dolist=['open','high','low']

        for curc in dolist:
            buffer=((df_all[curc]-df_all['pre_close'])*100)/df_all['pre_close']
            df_all[curc]=buffer
            df_all[curc]=df_all.groupby('trade_date')[curc].rank(pct=True)
            df_all[curc]=df_all[curc]*10//1

        #加入昨日rank
        df_all['yesterday_open']=df_all.groupby('ts_code')['open'].shift(1)
        df_all['yesterday_high']=df_all.groupby('ts_code')['high'].shift(1)
        df_all['yesterday_low']=df_all.groupby('ts_code')['low'].shift(1)
        df_all['yesterday_pst_amount_rank']=df_all.groupby('ts_code')['pst_amount_rank'].shift(1)
        #加入前日open
        df_all['yesterday2_open']=df_all.groupby('ts_code')['open'].shift(2)

        df_all.drop(['close','pre_close','pct_chg','pst_amount'],axis=1,inplace=True)
        #暂时不用的列
        df_all=df_all[df_all['high_stop']==0]
        #'tomorrow_chg'
        df_all.drop(['high_stop'],axis=1,inplace=True)



        df_all.dropna(axis=0,how='any',inplace=True)

        print(df_all)
        df_all=df_all.reset_index(drop=True)

        df_all.to_csv('today_train.csv')
        dwdw=1

    def data_feature(self,DataSetName):


        df_all=pd.read_csv(DataSetName,index_col=0,header=0)    
        
        start=df_all['trade_date'].apply(str)[0]
        end=df_all['trade_date'].apply(str)[df_all.shape[0]-1]
        xxx=pd.date_range(start,end)

        df = pd.DataFrame(xxx)
        df.columns = ['trade_date']
        df['trade_date']=df['trade_date'].map(str).map(lambda x : x[:4]+x[5:7]+x[8:10]).astype("int64")

        yyy=df_all['trade_date']
        zzz2=yyy.unique()
        df_2=pd.DataFrame(zzz2)
        df_2.columns = ['trade_date']
        df_2['day_flag']=1
    
        result = pd.merge(df, df_2, how='left', on=['trade_date'])
        result['day_flag2']=result['day_flag'].shift(-1)
        result['gap_day']=0

        result.loc[(result['day_flag']==1) & (result['day_flag2']!=1),'gap_day']=1

        result.drop(['day_flag','day_flag2'],axis=1,inplace=True)

        df_all=pd.merge(df_all, result, how='left', on=['trade_date'])

        #print(df_all)

        df_all.to_csv('datatest.csv')

        dsfsef=1

class FE3(FEbase):
    def __init__(self):
        pass

    def core(self,DataSetName):

        df_data=pd.read_csv(DataSetName[0],index_col=0,header=0)
        df_adj_all=pd.read_csv(DataSetName[1],index_col=0,header=0)
        #df_long_all=pd.read_csv(DataSetName[2],index_col=0,header=0)

        df_all=pd.merge(df_data, df_adj_all, how='inner', on=['ts_code','trade_date'])
        #df_all=pd.merge(df_all, df_long_all, how='inner', on=['ts_code','trade_date'])

        #===================================================================================================================================#
        #df_all['pe'] = df_all['pe'].fillna(9999)
        #df_all['pb'] = df_all['pb'].fillna(9999)

        #df_all['pe_rank']=df_all.groupby('trade_date')['pe'].rank(pct=True)
        #df_all['pb_rank']=df_all.groupby('trade_date')['pb'].rank(pct=True)        
        #df_all['pe_rank']=df_all['pe_rank']*10//1
        #df_all['pb_rank']=df_all['pb_rank']*10//1

        #df_all.drop(['turnover_rate','volume_ratio','pe','pb'],axis=1,inplace=True)

        #print(df_all)
        #df_all.to_csv('sjefosia.csv')

        #===================================================================================================================================#

        ##排除科创版
        #print(df_all)
        df_all=df_all[df_all['ts_code'].str.startswith('688')==False]
        

        #加入gap_day特征
        start=df_all['trade_date'].apply(str)[0]
        end=df_all['trade_date'].apply(str)[df_all.shape[0]-1]
        xxx=pd.date_range(start,end)

        df = pd.DataFrame(xxx)
        df.columns = ['trade_date']
        df['trade_date']=df['trade_date'].map(str).map(lambda x : x[:4]+x[5:7]+x[8:10]).astype("int64")

        yyy=df_all['trade_date']
        zzz2=yyy.unique()
        df_2=pd.DataFrame(zzz2)
        df_2.columns = ['trade_date']
        df_2['day_flag']=1
    
        result = pd.merge(df, df_2, how='left', on=['trade_date'])
        result['day_flag2']=result['day_flag'].shift(-1)
        result['gap_day']=0

        result.loc[(result['day_flag']==1) & (result['day_flag2']!=1),'gap_day']=1

        result.drop(['day_flag','day_flag2'],axis=1,inplace=True)

        df_all=pd.merge(df_all, result, how='left', on=['trade_date'])

        #===================================================================================================================================#

        #复权后价格
        df_all['real_price']=df_all['close']*df_all['adj_factor']
        
        #30日最低比值
        xxx=df_all.groupby('ts_code')['real_price'].rolling(30).min().reset_index()
        xxx.set_index(['level_1'], drop=True, append=False, inplace=True, verify_integrity=False)
        xxx.drop(['ts_code'],axis=1,inplace=True)
        
        df_all=df_all.join(xxx, lsuffix='', rsuffix='_30min')
        #bbb=df_all.groupby('ts_code')['real_price'].agg({'all_min':np.min, 'all_max': np.max}).reset_index()
        #ccc=pd.merge(df_all, bbb, how='inner', on=['ts_code'])
        df_all['30_pct']=(df_all['real_price']-df_all['real_price_30min'])/df_all['real_price_30min']
        df_all['30_pct_rank']=df_all.groupby('trade_date')['30_pct'].rank(pct=True)
        df_all['30_pct_rank']=df_all['30_pct_rank']*10//1

        #30日最高比值
        xxx=df_all.groupby('ts_code')['real_price'].rolling(30).max().reset_index()
        xxx.set_index(['level_1'], drop=True, append=False, inplace=True, verify_integrity=False)
        xxx.drop(['ts_code'],axis=1,inplace=True)
        
        df_all=df_all.join(xxx, lsuffix='', rsuffix='_30max')
        #bbb=df_all.groupby('ts_code')['real_price'].agg({'all_min':np.min, 'all_max': np.max}).reset_index()
        #ccc=pd.merge(df_all, bbb, how='inner', on=['ts_code'])
        df_all['30_pct_max']=(df_all['real_price']-df_all['real_price_30max'])/df_all['real_price_30max']
        df_all['30_pct_max_rank']=df_all.groupby('trade_date')['30_pct_max'].rank(pct=True)
        df_all['30_pct_max_rank']=df_all['30_pct_max_rank']*10//1

        df_all.drop(['30_pct','real_price_30min','30_pct_max','real_price_30max'],axis=1,inplace=True)

        df_all.drop(['change','vol'],axis=1,inplace=True)

        #===================================================================================================================================#
    
        #明日幅度
        #tm1=df_all.groupby('ts_code')['pct_chg'].shift(-1)
        #tm2=df_all.groupby('ts_code')['pct_chg'].shift(-2)
        #df_all['tomorrow_chg']=((100+tm1)*(100+tm2)-10000)/100
        #tm3=df_all.groupby('ts_code')['pct_chg'].shift(-3)
        #df_all['tomorrow_chg']=((100+tm1)*(100+tm2)*(100+tm3)-1000000)/10000
        df_all['tomorrow_chg']=df_all.groupby('ts_code')['pct_chg'].shift(-1)
        #明日排名
        df_all['tomorrow_chg_rank']=df_all.groupby('trade_date')['tomorrow_chg'].rank(pct=True)
        df_all['tomorrow_chg_rank']=df_all['tomorrow_chg_rank']*9.9//1
        #是否停
        df_all['high_stop']=0
        df_all.loc[df_all['pct_chg']>9,'high_stop']=1
        df_all.loc[(df_all['pct_chg']<5.4) & (4.6<df_all['pct_chg']),'high_stop']=1


        #真实价格范围(区分实际股价高低)
        df_all['price_real_rank']=df_all.groupby('trade_date')['pre_close'].rank(pct=True)
        df_all['price_real_rank']=df_all['price_real_rank']*10//1
        #1日
        df_all['chg_rank']=df_all.groupby('trade_date')['pct_chg'].rank(pct=True)
        df_all['chg_rank']=df_all['chg_rank']*10//1


        #6日
        xxx=df_all.groupby('ts_code')['chg_rank'].rolling(6).sum().reset_index()
        xxx.set_index(['level_1'], drop=True, append=False, inplace=True, verify_integrity=False)
        xxx.drop(['ts_code'],axis=1,inplace=True)

        df_all=df_all.join(xxx, lsuffix='', rsuffix='_6')

        df_all['chg_rank_6']=df_all.groupby('trade_date')['chg_rank_6'].rank(pct=True)
        df_all['chg_rank_6']=df_all['chg_rank_6']*10//1

        #10日
        xxx=df_all.groupby('ts_code')['chg_rank'].rolling(10).sum().reset_index()
        xxx.set_index(['level_1'], drop=True, append=False, inplace=True, verify_integrity=False)
        xxx.drop(['ts_code'],axis=1,inplace=True)

        df_all=df_all.join(xxx, lsuffix='', rsuffix='_10')

        df_all['chg_rank_10']=df_all.groupby('trade_date')['chg_rank_10'].rank(pct=True)
        df_all['chg_rank_10']=df_all['chg_rank_10']*10//1

        #3日
        xxx=df_all.groupby('ts_code')['chg_rank'].rolling(3).sum().reset_index()
        xxx.set_index(['level_1'], drop=True, append=False, inplace=True, verify_integrity=False)
        xxx.drop(['ts_code'],axis=1,inplace=True)

        df_all=df_all.join(xxx, lsuffix='', rsuffix='_3')

        df_all['chg_rank_3']=df_all.groupby('trade_date')['chg_rank_3'].rank(pct=True)
        df_all['chg_rank_3']=df_all['chg_rank_3']*10//1

        #print(df_all)

        #10日均量
        xxx=df_all.groupby('ts_code')['amount'].rolling(10).mean().reset_index()
        xxx.set_index(['level_1'], drop=True, append=False, inplace=True, verify_integrity=False)
        xxx.drop(['ts_code'],axis=1,inplace=True)
        df_all=df_all.join(xxx, lsuffix='_1', rsuffix='_10')

        #当日量占比
        df_all['pst_amount']=df_all['amount_1']/df_all['amount_10']
        df_all.drop(['amount_1','amount_10'],axis=1,inplace=True)
        #当日量排名
        df_all['pst_amount_rank']=df_all.groupby('trade_date')['pst_amount'].rank(pct=True)
        df_all['pst_amount_rank']=df_all['pst_amount_rank']*10//1

        #计算三种比例rank
        dolist=['open','high','low']

        for curc in dolist:
            buffer=((df_all[curc]-df_all['pre_close'])*100)/df_all['close']
            df_all[curc]=buffer
            df_all[curc]=df_all.groupby('trade_date')[curc].rank(pct=True)
            df_all[curc]=df_all[curc]*10//1

        #加入昨日rank
        df_all['yesterday_open']=df_all.groupby('ts_code')['open'].shift(1)
        df_all['yesterday_high']=df_all.groupby('ts_code')['high'].shift(1)
        df_all['yesterday_low']=df_all.groupby('ts_code')['low'].shift(1)
        df_all['yesterday_pst_amount_rank']=df_all.groupby('ts_code')['pst_amount_rank'].shift(1)
        #加入前日open
        df_all['yesterday2_open']=df_all.groupby('ts_code')['open'].shift(2)

        df_all.drop(['close','pre_close','pct_chg','pst_amount','adj_factor','real_price'],axis=1,inplace=True)
        #暂时不用的列
        df_all=df_all[df_all['high_stop']==0]

        #'tomorrow_chg'
        df_all.drop(['high_stop'],axis=1,inplace=True) 

        df_all.dropna(axis=0,how='any',inplace=True)

        print(df_all)
        df_all=df_all.reset_index(drop=True)

        return df_all

    def real_FE(self,gap_day):

        df_data=pd.read_csv('real_now.csv',index_col=0,header=0)
        df_adj_all=pd.read_csv('real_adj_now.csv',index_col=0,header=0)

        df_all=pd.merge(df_data, df_adj_all, how='left', on=['ts_code','trade_date'])

        #加入间隔
        df_all['gap_day']=gap_day

        #df_all=pd.read_csv(bufferstring,index_col=0,header=0,nrows=100000)
    
        #df_all.drop(['change','vol'],axis=1,inplace=True)
 

        #===================================================================================================================================#

        #复权后价格
        df_all['adj_factor']=df_all['adj_factor'].fillna(0)
        df_all['real_price']=df_all['close']*df_all['adj_factor']
        
        df_all['real_price']=df_all.groupby('ts_code')['real_price'].shift(1)
        df_all['real_price']=df_all['real_price']*(1+df_all['pct_chg']/100)


        #30日最低比值
        xxx=df_all.groupby('ts_code')['real_price'].rolling(30).min().reset_index()
        xxx.set_index(['level_1'], drop=True, append=False, inplace=True, verify_integrity=False)
        xxx.drop(['ts_code'],axis=1,inplace=True)
        
        df_all=df_all.join(xxx, lsuffix='', rsuffix='_30min')
        #bbb=df_all.groupby('ts_code')['real_price'].agg({'all_min':np.min, 'all_max': np.max}).reset_index()
        #ccc=pd.merge(df_all, bbb, how='inner', on=['ts_code'])
        df_all['30_pct']=(df_all['real_price']-df_all['real_price_30min'])/df_all['real_price_30min']
        df_all['30_pct_rank']=df_all.groupby('trade_date')['30_pct'].rank(pct=True)
        df_all['30_pct_rank']=df_all['30_pct_rank']*10//1

        #30日最高比值
        xxx=df_all.groupby('ts_code')['real_price'].rolling(30).max().reset_index()
        xxx.set_index(['level_1'], drop=True, append=False, inplace=True, verify_integrity=False)
        xxx.drop(['ts_code'],axis=1,inplace=True)
        
        df_all=df_all.join(xxx, lsuffix='', rsuffix='_30max')
        #bbb=df_all.groupby('ts_code')['real_price'].agg({'all_min':np.min, 'all_max': np.max}).reset_index()
        #ccc=pd.merge(df_all, bbb, how='inner', on=['ts_code'])
        df_all['30_pct_max']=(df_all['real_price']-df_all['real_price_30max'])/df_all['real_price_30max']
        df_all['30_pct_max_rank']=df_all.groupby('trade_date')['30_pct_max'].rank(pct=True)
        df_all['30_pct_max_rank']=df_all['30_pct_max_rank']*10//1

        df_all.drop(['30_pct','real_price_30min','30_pct_max','real_price_30max'],axis=1,inplace=True)


        #===================================================================================================================================#


        #是否停
        df_all['high_stop']=0
        df_all.loc[df_all['pct_chg']>9,'high_stop']=1
        df_all.loc[(df_all['pct_chg']<5.5) & (4.5<df_all['pct_chg']),'high_stop']=1


        #真实价格范围
        df_all['price_real_rank']=df_all.groupby('trade_date')['pre_close'].rank(pct=True)
        df_all['price_real_rank']=df_all['price_real_rank']*10//1
        #1日
        df_all['chg_rank']=df_all.groupby('trade_date')['pct_chg'].rank(pct=True)
        df_all['chg_rank']=df_all['chg_rank']*10//1


        #6日
        xxx=df_all.groupby('ts_code')['chg_rank'].rolling(6).sum().reset_index()
        xxx.set_index(['level_1'], drop=True, append=False, inplace=True, verify_integrity=False)
        xxx.drop(['ts_code'],axis=1,inplace=True)

        df_all=df_all.join(xxx, lsuffix='', rsuffix='_6')

        df_all['chg_rank_6']=df_all.groupby('trade_date')['chg_rank_6'].rank(pct=True)
        df_all['chg_rank_6']=df_all['chg_rank_6']*10//1

        #10日
        xxx=df_all.groupby('ts_code')['chg_rank'].rolling(10).sum().reset_index()
        xxx.set_index(['level_1'], drop=True, append=False, inplace=True, verify_integrity=False)
        xxx.drop(['ts_code'],axis=1,inplace=True)

        df_all=df_all.join(xxx, lsuffix='', rsuffix='_10')

        df_all['chg_rank_10']=df_all.groupby('trade_date')['chg_rank_10'].rank(pct=True)
        df_all['chg_rank_10']=df_all['chg_rank_10']*10//1

        #3日
        xxx=df_all.groupby('ts_code')['chg_rank'].rolling(3).sum().reset_index()
        xxx.set_index(['level_1'], drop=True, append=False, inplace=True, verify_integrity=False)
        xxx.drop(['ts_code'],axis=1,inplace=True)

        df_all=df_all.join(xxx, lsuffix='', rsuffix='_3')

        df_all['chg_rank_3']=df_all.groupby('trade_date')['chg_rank_3'].rank(pct=True)
        df_all['chg_rank_3']=df_all['chg_rank_3']*10//1

        #print(df_all)

        #10日均量
        xxx=df_all.groupby('ts_code')['amount'].rolling(10).mean().reset_index()
        xxx.set_index(['level_1'], drop=True, append=False, inplace=True, verify_integrity=False)
        xxx.drop(['ts_code'],axis=1,inplace=True)
        df_all=df_all.join(xxx, lsuffix='_1', rsuffix='_10')

        #当日量占比
        df_all['pst_amount']=df_all['amount_1']/df_all['amount_10']
        df_all.drop(['amount_1','amount_10'],axis=1,inplace=True)
        #当日量排名
        df_all['pst_amount_rank']=df_all.groupby('trade_date')['pst_amount'].rank(pct=True)
        df_all['pst_amount_rank']=df_all['pst_amount_rank']*10//1

        #计算三种比例rank
        dolist=['open','high','low']

        for curc in dolist:
            buffer=((df_all[curc]-df_all['pre_close'])*100)/df_all['pre_close']
            df_all[curc]=buffer
            df_all[curc]=df_all.groupby('trade_date')[curc].rank(pct=True)
            df_all[curc]=df_all[curc]*10//1

        #加入昨日rank
        df_all['yesterday_open']=df_all.groupby('ts_code')['open'].shift(1)
        df_all['yesterday_high']=df_all.groupby('ts_code')['high'].shift(1)
        df_all['yesterday_low']=df_all.groupby('ts_code')['low'].shift(1)
        df_all['yesterday_pst_amount_rank']=df_all.groupby('ts_code')['pst_amount_rank'].shift(1)
        #加入前日open
        df_all['yesterday2_open']=df_all.groupby('ts_code')['open'].shift(2)

        df_all.drop(['close','pre_close','pct_chg','pst_amount','adj_factor','real_price'],axis=1,inplace=True)
        #暂时不用的列
        df_all=df_all[df_all['high_stop']==0]
        #'tomorrow_chg'
        df_all.drop(['high_stop'],axis=1,inplace=True)



        df_all.dropna(axis=0,how='any',inplace=True)


        month_sec=df_all['trade_date'].max()
        df_all=df_all[df_all['trade_date']!=month_sec]
        print(df_all)
        df_all=df_all.reset_index(drop=True)

        df_all.to_csv('today_train.csv')
        dwdw=1

class FE4(FEbase):
    def __init__(self):
        pass

    def core(self,DataSetName):

        df_data=pd.read_csv(DataSetName[0],index_col=0,header=0)
        df_adj_all=pd.read_csv(DataSetName[1],index_col=0,header=0)
        df_long_all=pd.read_csv(DataSetName[2],index_col=0,header=0)

        df_all=pd.merge(df_data, df_adj_all, how='inner', on=['ts_code','trade_date'])
        df_all=pd.merge(df_all, df_long_all, how='inner', on=['ts_code','trade_date'])

        #===================================================================================================================================#
        df_all['pe'] = df_all['pe'].fillna(9999)
        df_all['pb'] = df_all['pb'].fillna(9999)

        df_all['pe_rank']=df_all.groupby('trade_date')['pe'].rank(pct=True)
        df_all['pb_rank']=df_all.groupby('trade_date')['pb'].rank(pct=True)        
        df_all['pe_rank']=df_all['pe_rank']*10//1
        df_all['pb_rank']=df_all['pb_rank']*10//1

        df_all.drop(['turnover_rate','volume_ratio','pe','pb'],axis=1,inplace=True)

        print(df_all)
        df_all.to_csv('sjefosia.csv')

        #===================================================================================================================================#

        #加入gap_day特征
        start=df_all['trade_date'].apply(str)[0]
        end=df_all['trade_date'].apply(str)[df_all.shape[0]-1]
        xxx=pd.date_range(start,end)

        df = pd.DataFrame(xxx)
        df.columns = ['trade_date']
        df['trade_date']=df['trade_date'].map(str).map(lambda x : x[:4]+x[5:7]+x[8:10]).astype("int64")

        yyy=df_all['trade_date']
        zzz2=yyy.unique()
        df_2=pd.DataFrame(zzz2)
        df_2.columns = ['trade_date']
        df_2['day_flag']=1
    
        result = pd.merge(df, df_2, how='left', on=['trade_date'])
        result['day_flag2']=result['day_flag'].shift(-1)
        result['gap_day']=0

        result.loc[(result['day_flag']==1) & (result['day_flag2']!=1),'gap_day']=1

        result.drop(['day_flag','day_flag2'],axis=1,inplace=True)

        df_all=pd.merge(df_all, result, how='left', on=['trade_date'])

        #===================================================================================================================================#

        #复权后价格
        df_all['real_price']=df_all['close']*df_all['adj_factor']
        
        #30日最低比值
        xxx=df_all.groupby('ts_code')['real_price'].rolling(30).min().reset_index()
        xxx.set_index(['level_1'], drop=True, append=False, inplace=True, verify_integrity=False)
        xxx.drop(['ts_code'],axis=1,inplace=True)
        
        df_all=df_all.join(xxx, lsuffix='', rsuffix='_30min')
        #bbb=df_all.groupby('ts_code')['real_price'].agg({'all_min':np.min, 'all_max': np.max}).reset_index()
        #ccc=pd.merge(df_all, bbb, how='inner', on=['ts_code'])
        df_all['30_pct']=(df_all['real_price']-df_all['real_price_30min'])/df_all['real_price_30min']
        df_all['30_pct_rank']=df_all.groupby('trade_date')['30_pct'].rank(pct=True)
        df_all['30_pct_rank']=df_all['30_pct_rank']*10//1

        #30日最高比值
        xxx=df_all.groupby('ts_code')['real_price'].rolling(30).max().reset_index()
        xxx.set_index(['level_1'], drop=True, append=False, inplace=True, verify_integrity=False)
        xxx.drop(['ts_code'],axis=1,inplace=True)
        
        df_all=df_all.join(xxx, lsuffix='', rsuffix='_30max')
        #bbb=df_all.groupby('ts_code')['real_price'].agg({'all_min':np.min, 'all_max': np.max}).reset_index()
        #ccc=pd.merge(df_all, bbb, how='inner', on=['ts_code'])
        df_all['30_pct_max']=(df_all['real_price']-df_all['real_price_30max'])/df_all['real_price_30max']
        df_all['30_pct_max_rank']=df_all.groupby('trade_date')['30_pct_max'].rank(pct=True)
        df_all['30_pct_max_rank']=df_all['30_pct_max_rank']*10//1

        df_all.drop(['30_pct','real_price_30min','30_pct_max','real_price_30max'],axis=1,inplace=True)

        df_all.drop(['change','vol'],axis=1,inplace=True)

        #===================================================================================================================================#
    
        #明日幅度
        #tm1=df_all.groupby('ts_code')['pct_chg'].shift(-1)
        #tm2=df_all.groupby('ts_code')['pct_chg'].shift(-2)
        #tm3=df_all.groupby('ts_code')['pct_chg'].shift(-3)
        #df_all['tomorrow_chg']=((100+tm1)*(100+tm2)*(100+tm3)-1000000)/10000
        df_all['tomorrow_chg']=df_all.groupby('ts_code')['pct_chg'].shift(-1)
        #明日排名
        df_all['tomorrow_chg_rank']=df_all.groupby('trade_date')['tomorrow_chg'].rank(pct=True)
        df_all['tomorrow_chg_rank']=df_all['tomorrow_chg_rank']*9.9//1
        #是否停
        df_all['high_stop']=0
        df_all.loc[df_all['pct_chg']>9,'high_stop']=1
        df_all.loc[(df_all['pct_chg']<5.4) & (4.6<df_all['pct_chg']),'high_stop']=1


        #真实价格范围(区分实际股价高低)
        df_all['price_real_rank']=df_all.groupby('trade_date')['pre_close'].rank(pct=True)
        df_all['price_real_rank']=df_all['price_real_rank']*10//1
        #1日
        df_all['chg_rank']=df_all.groupby('trade_date')['pct_chg'].rank(pct=True)
        df_all['chg_rank']=df_all['chg_rank']*10//1


        #6日
        xxx=df_all.groupby('ts_code')['chg_rank'].rolling(6).sum().reset_index()
        xxx.set_index(['level_1'], drop=True, append=False, inplace=True, verify_integrity=False)
        xxx.drop(['ts_code'],axis=1,inplace=True)

        df_all=df_all.join(xxx, lsuffix='', rsuffix='_6')

        df_all['chg_rank_6']=df_all.groupby('trade_date')['chg_rank_6'].rank(pct=True)
        df_all['chg_rank_6']=df_all['chg_rank_6']*10//1

        #10日
        xxx=df_all.groupby('ts_code')['chg_rank'].rolling(10).sum().reset_index()
        xxx.set_index(['level_1'], drop=True, append=False, inplace=True, verify_integrity=False)
        xxx.drop(['ts_code'],axis=1,inplace=True)

        df_all=df_all.join(xxx, lsuffix='', rsuffix='_10')

        df_all['chg_rank_10']=df_all.groupby('trade_date')['chg_rank_10'].rank(pct=True)
        df_all['chg_rank_10']=df_all['chg_rank_10']*10//1

        #3日
        xxx=df_all.groupby('ts_code')['chg_rank'].rolling(3).sum().reset_index()
        xxx.set_index(['level_1'], drop=True, append=False, inplace=True, verify_integrity=False)
        xxx.drop(['ts_code'],axis=1,inplace=True)

        df_all=df_all.join(xxx, lsuffix='', rsuffix='_3')

        df_all['chg_rank_3']=df_all.groupby('trade_date')['chg_rank_3'].rank(pct=True)
        df_all['chg_rank_3']=df_all['chg_rank_3']*10//1

        #print(df_all)

        #10日均量
        xxx=df_all.groupby('ts_code')['amount'].rolling(10).mean().reset_index()
        xxx.set_index(['level_1'], drop=True, append=False, inplace=True, verify_integrity=False)
        xxx.drop(['ts_code'],axis=1,inplace=True)
        df_all=df_all.join(xxx, lsuffix='_1', rsuffix='_10')

        #当日量占比
        df_all['pst_amount']=df_all['amount_1']/df_all['amount_10']
        df_all.drop(['amount_1','amount_10'],axis=1,inplace=True)
        #当日量排名
        df_all['pst_amount_rank']=df_all.groupby('trade_date')['pst_amount'].rank(pct=True)
        df_all['pst_amount_rank']=df_all['pst_amount_rank']*10//1

        #计算三种比例rank
        dolist=['open','high','low']

        for curc in dolist:
            buffer=((df_all[curc]-df_all['pre_close'])*100)/df_all['close']
            df_all[curc]=buffer
            df_all[curc]=df_all.groupby('trade_date')[curc].rank(pct=True)
            df_all[curc]=df_all[curc]*10//1

        #加入昨日rank
        df_all['yesterday_open']=df_all.groupby('ts_code')['open'].shift(1)
        df_all['yesterday_high']=df_all.groupby('ts_code')['high'].shift(1)
        df_all['yesterday_low']=df_all.groupby('ts_code')['low'].shift(1)
        df_all['yesterday_pst_amount_rank']=df_all.groupby('ts_code')['pst_amount_rank'].shift(1)
        #加入前日open
        df_all['yesterday2_open']=df_all.groupby('ts_code')['open'].shift(2)

        df_all.drop(['close','pre_close','pct_chg','pst_amount','adj_factor','real_price'],axis=1,inplace=True)
        #暂时不用的列
        df_all=df_all[df_all['high_stop']==0]
        #'tomorrow_chg'
        df_all.drop(['high_stop'],axis=1,inplace=True) 

        df_all.dropna(axis=0,how='any',inplace=True)

        print(df_all)
        df_all=df_all.reset_index(drop=True)

        return df_all

    def real_FE(self,gap_day):

        df_data=pd.read_csv('real_now.csv',index_col=0,header=0)
        df_adj_all=pd.read_csv('real_adj_now.csv',index_col=0,header=0)

        df_all=pd.merge(df_data, df_adj_all, how='left', on=['ts_code','trade_date'])

        #加入间隔
        df_all['gap_day']=gap_day

        #df_all=pd.read_csv(bufferstring,index_col=0,header=0,nrows=100000)
    
        #df_all.drop(['change','vol'],axis=1,inplace=True)
 

        #===================================================================================================================================#

        #复权后价格
        df_all['adj_factor']=df_all['adj_factor'].fillna(0)
        df_all['real_price']=df_all['close']*df_all['adj_factor']
        
        df_all['real_price']=df_all.groupby('ts_code')['real_price'].shift(1)
        df_all['real_price']=df_all['real_price']*(1+df_all['pct_chg']/100)


        #30日最低比值
        xxx=df_all.groupby('ts_code')['real_price'].rolling(30).min().reset_index()
        xxx.set_index(['level_1'], drop=True, append=False, inplace=True, verify_integrity=False)
        xxx.drop(['ts_code'],axis=1,inplace=True)
        
        df_all=df_all.join(xxx, lsuffix='', rsuffix='_30min')
        #bbb=df_all.groupby('ts_code')['real_price'].agg({'all_min':np.min, 'all_max': np.max}).reset_index()
        #ccc=pd.merge(df_all, bbb, how='inner', on=['ts_code'])
        df_all['30_pct']=(df_all['real_price']-df_all['real_price_30min'])/df_all['real_price_30min']
        df_all['30_pct_rank']=df_all.groupby('trade_date')['30_pct'].rank(pct=True)
        df_all['30_pct_rank']=df_all['30_pct_rank']*10//1

        #30日最高比值
        xxx=df_all.groupby('ts_code')['real_price'].rolling(30).max().reset_index()
        xxx.set_index(['level_1'], drop=True, append=False, inplace=True, verify_integrity=False)
        xxx.drop(['ts_code'],axis=1,inplace=True)
        
        df_all=df_all.join(xxx, lsuffix='', rsuffix='_30max')
        #bbb=df_all.groupby('ts_code')['real_price'].agg({'all_min':np.min, 'all_max': np.max}).reset_index()
        #ccc=pd.merge(df_all, bbb, how='inner', on=['ts_code'])
        df_all['30_pct_max']=(df_all['real_price']-df_all['real_price_30max'])/df_all['real_price_30max']
        df_all['30_pct_max_rank']=df_all.groupby('trade_date')['30_pct_max'].rank(pct=True)
        df_all['30_pct_max_rank']=df_all['30_pct_max_rank']*10//1

        df_all.drop(['30_pct','real_price_30min','30_pct_max','real_price_30max'],axis=1,inplace=True)


        #===================================================================================================================================#


        #是否停
        df_all['high_stop']=0
        df_all.loc[df_all['pct_chg']>9,'high_stop']=1
        df_all.loc[(df_all['pct_chg']<5.5) & (4.5<df_all['pct_chg']),'high_stop']=1


        #真实价格范围
        df_all['price_real_rank']=df_all.groupby('trade_date')['pre_close'].rank(pct=True)
        df_all['price_real_rank']=df_all['price_real_rank']*10//1
        #1日
        df_all['chg_rank']=df_all.groupby('trade_date')['pct_chg'].rank(pct=True)
        df_all['chg_rank']=df_all['chg_rank']*10//1


        #6日
        xxx=df_all.groupby('ts_code')['chg_rank'].rolling(6).sum().reset_index()
        xxx.set_index(['level_1'], drop=True, append=False, inplace=True, verify_integrity=False)
        xxx.drop(['ts_code'],axis=1,inplace=True)

        df_all=df_all.join(xxx, lsuffix='', rsuffix='_6')

        df_all['chg_rank_6']=df_all.groupby('trade_date')['chg_rank_6'].rank(pct=True)
        df_all['chg_rank_6']=df_all['chg_rank_6']*10//1

        #10日
        xxx=df_all.groupby('ts_code')['chg_rank'].rolling(10).sum().reset_index()
        xxx.set_index(['level_1'], drop=True, append=False, inplace=True, verify_integrity=False)
        xxx.drop(['ts_code'],axis=1,inplace=True)

        df_all=df_all.join(xxx, lsuffix='', rsuffix='_10')

        df_all['chg_rank_10']=df_all.groupby('trade_date')['chg_rank_10'].rank(pct=True)
        df_all['chg_rank_10']=df_all['chg_rank_10']*10//1

        #3日
        xxx=df_all.groupby('ts_code')['chg_rank'].rolling(3).sum().reset_index()
        xxx.set_index(['level_1'], drop=True, append=False, inplace=True, verify_integrity=False)
        xxx.drop(['ts_code'],axis=1,inplace=True)

        df_all=df_all.join(xxx, lsuffix='', rsuffix='_3')

        df_all['chg_rank_3']=df_all.groupby('trade_date')['chg_rank_3'].rank(pct=True)
        df_all['chg_rank_3']=df_all['chg_rank_3']*10//1

        #print(df_all)

        #10日均量
        xxx=df_all.groupby('ts_code')['amount'].rolling(10).mean().reset_index()
        xxx.set_index(['level_1'], drop=True, append=False, inplace=True, verify_integrity=False)
        xxx.drop(['ts_code'],axis=1,inplace=True)
        df_all=df_all.join(xxx, lsuffix='_1', rsuffix='_10')

        #当日量占比
        df_all['pst_amount']=df_all['amount_1']/df_all['amount_10']
        df_all.drop(['amount_1','amount_10'],axis=1,inplace=True)
        #当日量排名
        df_all['pst_amount_rank']=df_all.groupby('trade_date')['pst_amount'].rank(pct=True)
        df_all['pst_amount_rank']=df_all['pst_amount_rank']*10//1

        #计算三种比例rank
        dolist=['open','high','low']

        for curc in dolist:
            buffer=((df_all[curc]-df_all['pre_close'])*100)/df_all['pre_close']
            df_all[curc]=buffer
            df_all[curc]=df_all.groupby('trade_date')[curc].rank(pct=True)
            df_all[curc]=df_all[curc]*10//1

        #加入昨日rank
        df_all['yesterday_open']=df_all.groupby('ts_code')['open'].shift(1)
        df_all['yesterday_high']=df_all.groupby('ts_code')['high'].shift(1)
        df_all['yesterday_low']=df_all.groupby('ts_code')['low'].shift(1)
        df_all['yesterday_pst_amount_rank']=df_all.groupby('ts_code')['pst_amount_rank'].shift(1)
        #加入前日open
        df_all['yesterday2_open']=df_all.groupby('ts_code')['open'].shift(2)

        df_all.drop(['close','pre_close','pct_chg','pst_amount','adj_factor','real_price'],axis=1,inplace=True)
        #暂时不用的列
        df_all=df_all[df_all['high_stop']==0]
        #'tomorrow_chg'
        df_all.drop(['high_stop'],axis=1,inplace=True)



        df_all.dropna(axis=0,how='any',inplace=True)

        print(df_all)
        df_all=df_all.reset_index(drop=True)

        df_all.to_csv('today_train.csv')
        dwdw=1

    def data_feature(self,DataSetName):


        df_all=pd.read_csv(DataSetName,index_col=0,header=0)    
        
        start=df_all['trade_date'].apply(str)[0]
        end=df_all['trade_date'].apply(str)[df_all.shape[0]-1]
        xxx=pd.date_range(start,end)

        df = pd.DataFrame(xxx)
        df.columns = ['trade_date']
        df['trade_date']=df['trade_date'].map(str).map(lambda x : x[:4]+x[5:7]+x[8:10]).astype("int64")

        yyy=df_all['trade_date']
        zzz2=yyy.unique()
        df_2=pd.DataFrame(zzz2)
        df_2.columns = ['trade_date']
        df_2['day_flag']=1
    
        result = pd.merge(df, df_2, how='left', on=['trade_date'])
        result['day_flag2']=result['day_flag'].shift(-1)
        result['gap_day']=0

        result.loc[(result['day_flag']==1) & (result['day_flag2']!=1),'gap_day']=1

        result.drop(['day_flag','day_flag2'],axis=1,inplace=True)

        df_all=pd.merge(df_all, result, how='left', on=['trade_date'])

        #print(df_all)

        df_all.to_csv('datatest.csv')

        dsfsef=1

class FE5(FEbase):
    def __init__(self):
        pass

    def core(self,DataSetName):

        df_data=pd.read_csv(DataSetName[0],index_col=0,header=0)
        df_adj_all=pd.read_csv(DataSetName[1],index_col=0,header=0)
        df_long_all=pd.read_csv(DataSetName[2],index_col=0,header=0)

        df_all=pd.merge(df_data, df_adj_all, how='inner', on=['ts_code','trade_date'])
        #df_all=pd.merge(df_all, df_long_all, how='inner', on=['ts_code','trade_date'])

        #===================================================================================================================================#
        #df_all['pe'] = df_all['pe'].fillna(9999)
        #df_all['pb'] = df_all['pb'].fillna(9999)

        #df_all['pe_rank']=df_all.groupby('trade_date')['pe'].rank(pct=True)
        #df_all['pb_rank']=df_all.groupby('trade_date')['pb'].rank(pct=True)        
        #df_all['pe_rank']=df_all['pe_rank']*10//1
        #df_all['pb_rank']=df_all['pb_rank']*10//1

        #df_all.drop(['turnover_rate','volume_ratio','pe','pb'],axis=1,inplace=True)

        #print(df_all)
        #df_all.to_csv('sjefosia.csv')

        #===================================================================================================================================#

        #加入gap_day特征
        start=df_all['trade_date'].apply(str)[0]
        end=df_all['trade_date'].apply(str)[df_all.shape[0]-1]
        xxx=pd.date_range(start,end)

        df = pd.DataFrame(xxx)
        df.columns = ['trade_date']
        df['trade_date']=df['trade_date'].map(str).map(lambda x : x[:4]+x[5:7]+x[8:10]).astype("int64")

        yyy=df_all['trade_date']
        zzz2=yyy.unique()
        df_2=pd.DataFrame(zzz2)
        df_2.columns = ['trade_date']
        df_2['day_flag']=1
    
        result = pd.merge(df, df_2, how='left', on=['trade_date'])
        result['day_flag2']=result['day_flag'].shift(-1)
        result['gap_day']=0

        result.loc[(result['day_flag']==1) & (result['day_flag2']!=1),'gap_day']=1

        result.drop(['day_flag','day_flag2'],axis=1,inplace=True)

        df_all=pd.merge(df_all, result, how='left', on=['trade_date'])

        #===================================================================================================================================#

        #复权后价格
        df_all['real_price']=df_all['close']*df_all['adj_factor']
        
        #30日最低比值
        xxx=df_all.groupby('ts_code')['real_price'].rolling(30).min().reset_index()
        xxx.set_index(['level_1'], drop=True, append=False, inplace=True, verify_integrity=False)
        xxx.drop(['ts_code'],axis=1,inplace=True)
        
        df_all=df_all.join(xxx, lsuffix='', rsuffix='_30min')
        #bbb=df_all.groupby('ts_code')['real_price'].agg({'all_min':np.min, 'all_max': np.max}).reset_index()
        #ccc=pd.merge(df_all, bbb, how='inner', on=['ts_code'])
        df_all['30_pct']=(df_all['real_price']-df_all['real_price_30min'])/df_all['real_price_30min']
        df_all['30_pct_rank']=df_all.groupby('trade_date')['30_pct'].rank(pct=True)
        df_all['30_pct_rank']=df_all['30_pct_rank']*10//1

        #30日最高比值
        xxx=df_all.groupby('ts_code')['real_price'].rolling(30).max().reset_index()
        xxx.set_index(['level_1'], drop=True, append=False, inplace=True, verify_integrity=False)
        xxx.drop(['ts_code'],axis=1,inplace=True)
        
        df_all=df_all.join(xxx, lsuffix='', rsuffix='_30max')
        #bbb=df_all.groupby('ts_code')['real_price'].agg({'all_min':np.min, 'all_max': np.max}).reset_index()
        #ccc=pd.merge(df_all, bbb, how='inner', on=['ts_code'])
        df_all['30_pct_max']=(df_all['real_price']-df_all['real_price_30max'])/df_all['real_price_30max']
        df_all['30_pct_max_rank']=df_all.groupby('trade_date')['30_pct_max'].rank(pct=True)
        df_all['30_pct_max_rank']=df_all['30_pct_max_rank']*10//1

        df_all.drop(['30_pct','real_price_30min','30_pct_max','real_price_30max'],axis=1,inplace=True)

        df_all.drop(['change','vol'],axis=1,inplace=True)

        #===================================================================================================================================#
    
        #明日幅度
        #tm1=df_all.groupby('ts_code')['pct_chg'].shift(-1)
        #tm2=df_all.groupby('ts_code')['pct_chg'].shift(-2)
        #tm3=df_all.groupby('ts_code')['pct_chg'].shift(-3)
        #df_all['tomorrow_chg']=((100+tm1)*(100+tm2)*(100+tm3)-1000000)/10000
        df_all['tomorrow_chg']=df_all.groupby('ts_code')['pct_chg'].shift(-1)
        #明日排名
        df_all['tomorrow_chg_rank']=df_all.groupby('trade_date')['tomorrow_chg'].rank(pct=True)
        df_all['tomorrow_chg_rank']=df_all['tomorrow_chg_rank']*9.9//1
        #是否停
        df_all['high_stop']=0
        df_all.loc[df_all['pct_chg']>9,'high_stop']=1
        df_all.loc[(df_all['pct_chg']<5.2) & (4.8<df_all['pct_chg']),'high_stop']=1
        df_all.loc[df_all['tomorrow_chg']>9.7,'tomorrow_chg_rank']=10
        #print(df_all['tomorrow_chg_rank']==10)
        df_all.loc[df_all['tomorrow_chg_rank']>4,'tomorrow_chg_rank']-=1

        #真实价格范围(区分实际股价高低)
        df_all['price_real_rank']=df_all.groupby('trade_date')['pre_close'].rank(pct=True)
        df_all['price_real_rank']=df_all['price_real_rank']*10//1
        #1日
        df_all['chg_rank']=df_all.groupby('trade_date')['pct_chg'].rank(pct=True)
        df_all['chg_rank']=df_all['chg_rank']*10//1


        #6日
        xxx=df_all.groupby('ts_code')['chg_rank'].rolling(6).sum().reset_index()
        xxx.set_index(['level_1'], drop=True, append=False, inplace=True, verify_integrity=False)
        xxx.drop(['ts_code'],axis=1,inplace=True)

        df_all=df_all.join(xxx, lsuffix='', rsuffix='_6')

        df_all['chg_rank_6']=df_all.groupby('trade_date')['chg_rank_6'].rank(pct=True)
        df_all['chg_rank_6']=df_all['chg_rank_6']*10//1

        #10日
        xxx=df_all.groupby('ts_code')['chg_rank'].rolling(10).sum().reset_index()
        xxx.set_index(['level_1'], drop=True, append=False, inplace=True, verify_integrity=False)
        xxx.drop(['ts_code'],axis=1,inplace=True)

        df_all=df_all.join(xxx, lsuffix='', rsuffix='_10')

        df_all['chg_rank_10']=df_all.groupby('trade_date')['chg_rank_10'].rank(pct=True)
        df_all['chg_rank_10']=df_all['chg_rank_10']*10//1

        #3日
        xxx=df_all.groupby('ts_code')['chg_rank'].rolling(3).sum().reset_index()
        xxx.set_index(['level_1'], drop=True, append=False, inplace=True, verify_integrity=False)
        xxx.drop(['ts_code'],axis=1,inplace=True)

        df_all=df_all.join(xxx, lsuffix='', rsuffix='_3')

        df_all['chg_rank_3']=df_all.groupby('trade_date')['chg_rank_3'].rank(pct=True)
        df_all['chg_rank_3']=df_all['chg_rank_3']*10//1

        #print(df_all)

        #10日均量
        xxx=df_all.groupby('ts_code')['amount'].rolling(10).mean().reset_index()
        xxx.set_index(['level_1'], drop=True, append=False, inplace=True, verify_integrity=False)
        xxx.drop(['ts_code'],axis=1,inplace=True)
        df_all=df_all.join(xxx, lsuffix='_1', rsuffix='_10')

        #当日量占比
        df_all['pst_amount']=df_all['amount_1']/df_all['amount_10']
        df_all.drop(['amount_1','amount_10'],axis=1,inplace=True)
        #当日量排名
        df_all['pst_amount_rank']=df_all.groupby('trade_date')['pst_amount'].rank(pct=True)
        df_all['pst_amount_rank']=df_all['pst_amount_rank']*10//1

        #计算三种比例rank
        dolist=['open','high','low']

        for curc in dolist:
            buffer=((df_all[curc]-df_all['pre_close'])*100)/df_all['close']
            df_all[curc]=buffer
            df_all[curc]=df_all.groupby('trade_date')[curc].rank(pct=True)
            df_all[curc]=df_all[curc]*10//1

        #加入昨日rank
        df_all['yesterday_open']=df_all.groupby('ts_code')['open'].shift(1)
        df_all['yesterday_high']=df_all.groupby('ts_code')['high'].shift(1)
        df_all['yesterday_low']=df_all.groupby('ts_code')['low'].shift(1)
        df_all['yesterday_pst_amount_rank']=df_all.groupby('ts_code')['pst_amount_rank'].shift(1)
        #加入前日open
        df_all['yesterday2_open']=df_all.groupby('ts_code')['open'].shift(2)

        df_all.drop(['close','pre_close','pct_chg','pst_amount','adj_factor','real_price'],axis=1,inplace=True)
        #暂时不用的列
        df_all=df_all[df_all['high_stop']==0]
        #'tomorrow_chg'
        df_all.drop(['high_stop'],axis=1,inplace=True) 

        df_all.dropna(axis=0,how='any',inplace=True)

        print(df_all)
        df_all=df_all.reset_index(drop=True)

        return df_all

    def real_FE(self,gap_day):

        df_data=pd.read_csv('real_now.csv',index_col=0,header=0)
        df_adj_all=pd.read_csv('real_adj_now.csv',index_col=0,header=0)

        df_all=pd.merge(df_data, df_adj_all, how='left', on=['ts_code','trade_date'])

        #加入间隔
        df_all['gap_day']=gap_day

        #df_all=pd.read_csv(bufferstring,index_col=0,header=0,nrows=100000)
    
        #df_all.drop(['change','vol'],axis=1,inplace=True)
 

        #===================================================================================================================================#

        #复权后价格
        df_all['adj_factor']=df_all['adj_factor'].fillna(0)
        df_all['real_price']=df_all['close']*df_all['adj_factor']
        
        df_all['real_price']=df_all.groupby('ts_code')['real_price'].shift(1)
        df_all['real_price']=df_all['real_price']*(1+df_all['pct_chg']/100)


        #30日最低比值
        xxx=df_all.groupby('ts_code')['real_price'].rolling(30).min().reset_index()
        xxx.set_index(['level_1'], drop=True, append=False, inplace=True, verify_integrity=False)
        xxx.drop(['ts_code'],axis=1,inplace=True)
        
        df_all=df_all.join(xxx, lsuffix='', rsuffix='_30min')
        #bbb=df_all.groupby('ts_code')['real_price'].agg({'all_min':np.min, 'all_max': np.max}).reset_index()
        #ccc=pd.merge(df_all, bbb, how='inner', on=['ts_code'])
        df_all['30_pct']=(df_all['real_price']-df_all['real_price_30min'])/df_all['real_price_30min']
        df_all['30_pct_rank']=df_all.groupby('trade_date')['30_pct'].rank(pct=True)
        df_all['30_pct_rank']=df_all['30_pct_rank']*10//1

        #30日最高比值
        xxx=df_all.groupby('ts_code')['real_price'].rolling(30).max().reset_index()
        xxx.set_index(['level_1'], drop=True, append=False, inplace=True, verify_integrity=False)
        xxx.drop(['ts_code'],axis=1,inplace=True)
        
        df_all=df_all.join(xxx, lsuffix='', rsuffix='_30max')
        #bbb=df_all.groupby('ts_code')['real_price'].agg({'all_min':np.min, 'all_max': np.max}).reset_index()
        #ccc=pd.merge(df_all, bbb, how='inner', on=['ts_code'])
        df_all['30_pct_max']=(df_all['real_price']-df_all['real_price_30max'])/df_all['real_price_30max']
        df_all['30_pct_max_rank']=df_all.groupby('trade_date')['30_pct_max'].rank(pct=True)
        df_all['30_pct_max_rank']=df_all['30_pct_max_rank']*10//1

        df_all.drop(['30_pct','real_price_30min','30_pct_max','real_price_30max'],axis=1,inplace=True)


        #===================================================================================================================================#


        #是否停
        df_all['high_stop']=0
        df_all.loc[df_all['pct_chg']>9,'high_stop']=1
        df_all.loc[(df_all['pct_chg']<5.5) & (4.5<df_all['pct_chg']),'high_stop']=1


        #真实价格范围
        df_all['price_real_rank']=df_all.groupby('trade_date')['pre_close'].rank(pct=True)
        df_all['price_real_rank']=df_all['price_real_rank']*10//1
        #1日
        df_all['chg_rank']=df_all.groupby('trade_date')['pct_chg'].rank(pct=True)
        df_all['chg_rank']=df_all['chg_rank']*10//1


        #6日
        xxx=df_all.groupby('ts_code')['chg_rank'].rolling(6).sum().reset_index()
        xxx.set_index(['level_1'], drop=True, append=False, inplace=True, verify_integrity=False)
        xxx.drop(['ts_code'],axis=1,inplace=True)

        df_all=df_all.join(xxx, lsuffix='', rsuffix='_6')

        df_all['chg_rank_6']=df_all.groupby('trade_date')['chg_rank_6'].rank(pct=True)
        df_all['chg_rank_6']=df_all['chg_rank_6']*10//1

        #10日
        xxx=df_all.groupby('ts_code')['chg_rank'].rolling(10).sum().reset_index()
        xxx.set_index(['level_1'], drop=True, append=False, inplace=True, verify_integrity=False)
        xxx.drop(['ts_code'],axis=1,inplace=True)

        df_all=df_all.join(xxx, lsuffix='', rsuffix='_10')

        df_all['chg_rank_10']=df_all.groupby('trade_date')['chg_rank_10'].rank(pct=True)
        df_all['chg_rank_10']=df_all['chg_rank_10']*10//1

        #3日
        xxx=df_all.groupby('ts_code')['chg_rank'].rolling(3).sum().reset_index()
        xxx.set_index(['level_1'], drop=True, append=False, inplace=True, verify_integrity=False)
        xxx.drop(['ts_code'],axis=1,inplace=True)

        df_all=df_all.join(xxx, lsuffix='', rsuffix='_3')

        df_all['chg_rank_3']=df_all.groupby('trade_date')['chg_rank_3'].rank(pct=True)
        df_all['chg_rank_3']=df_all['chg_rank_3']*10//1

        #print(df_all)

        #10日均量
        xxx=df_all.groupby('ts_code')['amount'].rolling(10).mean().reset_index()
        xxx.set_index(['level_1'], drop=True, append=False, inplace=True, verify_integrity=False)
        xxx.drop(['ts_code'],axis=1,inplace=True)
        df_all=df_all.join(xxx, lsuffix='_1', rsuffix='_10')

        #当日量占比
        df_all['pst_amount']=df_all['amount_1']/df_all['amount_10']
        df_all.drop(['amount_1','amount_10'],axis=1,inplace=True)
        #当日量排名
        df_all['pst_amount_rank']=df_all.groupby('trade_date')['pst_amount'].rank(pct=True)
        df_all['pst_amount_rank']=df_all['pst_amount_rank']*10//1

        #计算三种比例rank
        dolist=['open','high','low']

        for curc in dolist:
            buffer=((df_all[curc]-df_all['pre_close'])*100)/df_all['pre_close']
            df_all[curc]=buffer
            df_all[curc]=df_all.groupby('trade_date')[curc].rank(pct=True)
            df_all[curc]=df_all[curc]*10//1

        #加入昨日rank
        df_all['yesterday_open']=df_all.groupby('ts_code')['open'].shift(1)
        df_all['yesterday_high']=df_all.groupby('ts_code')['high'].shift(1)
        df_all['yesterday_low']=df_all.groupby('ts_code')['low'].shift(1)
        df_all['yesterday_pst_amount_rank']=df_all.groupby('ts_code')['pst_amount_rank'].shift(1)
        #加入前日open
        df_all['yesterday2_open']=df_all.groupby('ts_code')['open'].shift(2)

        df_all.drop(['close','pre_close','pct_chg','pst_amount','adj_factor','real_price'],axis=1,inplace=True)
        #暂时不用的列
        df_all=df_all[df_all['high_stop']==0]
        #'tomorrow_chg'
        df_all.drop(['high_stop'],axis=1,inplace=True)



        df_all.dropna(axis=0,how='any',inplace=True)


        month_sec=df_all['trade_date'].max()
        df_all=df_all[df_all['trade_date']!=month_sec]
        print(df_all)
        df_all=df_all.reset_index(drop=True)

        df_all.to_csv('today_train.csv')
        dwdw=1

    def data_feature(self,DataSetName):


        df_all=pd.read_csv(DataSetName,index_col=0,header=0)    
        
        start=df_all['trade_date'].apply(str)[0]
        end=df_all['trade_date'].apply(str)[df_all.shape[0]-1]
        xxx=pd.date_range(start,end)

        df = pd.DataFrame(xxx)
        df.columns = ['trade_date']
        df['trade_date']=df['trade_date'].map(str).map(lambda x : x[:4]+x[5:7]+x[8:10]).astype("int64")

        yyy=df_all['trade_date']
        zzz2=yyy.unique()
        df_2=pd.DataFrame(zzz2)
        df_2.columns = ['trade_date']
        df_2['day_flag']=1
    
        result = pd.merge(df, df_2, how='left', on=['trade_date'])
        result['day_flag2']=result['day_flag'].shift(-1)
        result['gap_day']=0

        result.loc[(result['day_flag']==1) & (result['day_flag2']!=1),'gap_day']=1

        result.drop(['day_flag','day_flag2'],axis=1,inplace=True)

        df_all=pd.merge(df_all, result, how='left', on=['trade_date'])

        #print(df_all)

        df_all.to_csv('datatest.csv')

        dsfsef=1

class FE6(FEbase):
    def __init__(self):
        pass

    def core(self,DataSetName):

        df_data=pd.read_csv(DataSetName[0],index_col=0,header=0)
        df_adj_all=pd.read_csv(DataSetName[1],index_col=0,header=0)
        df_long_all=pd.read_csv(DataSetName[2],index_col=0,header=0)

        df_all=pd.merge(df_data, df_adj_all, how='inner', on=['ts_code','trade_date'])
        #df_all=pd.merge(df_all, df_long_all, how='inner', on=['ts_code','trade_date'])

        #===================================================================================================================================#
        #df_all['pe'] = df_all['pe'].fillna(9999)
        #df_all['pb'] = df_all['pb'].fillna(9999)

        #df_all['pe_rank']=df_all.groupby('trade_date')['pe'].rank(pct=True)
        #df_all['pb_rank']=df_all.groupby('trade_date')['pb'].rank(pct=True)        
        #df_all['pe_rank']=df_all['pe_rank']*10//1
        #df_all['pb_rank']=df_all['pb_rank']*10//1

        #df_all.drop(['turnover_rate','volume_ratio','pe','pb'],axis=1,inplace=True)

        #print(df_all)
        #df_all.to_csv('sjefosia.csv')

        #===================================================================================================================================#

        #加入gap_day特征
        start=df_all['trade_date'].apply(str)[0]
        end=df_all['trade_date'].apply(str)[df_all.shape[0]-1]
        xxx=pd.date_range(start,end)

        df = pd.DataFrame(xxx)
        df.columns = ['trade_date']
        df['trade_date']=df['trade_date'].map(str).map(lambda x : x[:4]+x[5:7]+x[8:10]).astype("int64")

        yyy=df_all['trade_date']
        zzz2=yyy.unique()
        df_2=pd.DataFrame(zzz2)
        df_2.columns = ['trade_date']
        df_2['day_flag']=1
    
        result = pd.merge(df, df_2, how='left', on=['trade_date'])
        result['day_flag2']=result['day_flag'].shift(-1)
        result['gap_day']=0

        result.loc[(result['day_flag']==1) & (result['day_flag2']!=1),'gap_day']=1

        result.drop(['day_flag','day_flag2'],axis=1,inplace=True)

        df_all=pd.merge(df_all, result, how='left', on=['trade_date'])

        #===================================================================================================================================#

        ##复权后价格
        df_all['real_price']=df_all['close']*df_all['adj_factor']
        
        ##30日最低比值
        #xxx=df_all.groupby('ts_code')['real_price'].rolling(30).min().reset_index()
        #xxx.set_index(['level_1'], drop=True, append=False, inplace=True, verify_integrity=False)
        #xxx.drop(['ts_code'],axis=1,inplace=True)
        
        #df_all=df_all.join(xxx, lsuffix='', rsuffix='_30min')
        ##bbb=df_all.groupby('ts_code')['real_price'].agg({'all_min':np.min, 'all_max': np.max}).reset_index()
        ##ccc=pd.merge(df_all, bbb, how='inner', on=['ts_code'])
        #df_all['30_pct']=(df_all['real_price']-df_all['real_price_30min'])/df_all['real_price_30min']
        #df_all['30_pct_rank']=df_all.groupby('trade_date')['30_pct'].rank(pct=True)
        #df_all['30_pct_rank']=df_all['30_pct_rank']*10//1

        ##30日最高比值
        #xxx=df_all.groupby('ts_code')['real_price'].rolling(30).max().reset_index()
        #xxx.set_index(['level_1'], drop=True, append=False, inplace=True, verify_integrity=False)
        #xxx.drop(['ts_code'],axis=1,inplace=True)
        
        #df_all=df_all.join(xxx, lsuffix='', rsuffix='_30max')
        ##bbb=df_all.groupby('ts_code')['real_price'].agg({'all_min':np.min, 'all_max': np.max}).reset_index()
        ##ccc=pd.merge(df_all, bbb, how='inner', on=['ts_code'])
        #df_all['30_pct_max']=(df_all['real_price']-df_all['real_price_30max'])/df_all['real_price_30max']
        #df_all['30_pct_max_rank']=df_all.groupby('trade_date')['30_pct_max'].rank(pct=True)
        #df_all['30_pct_max_rank']=df_all['30_pct_max_rank']*10//1

        #df_all.drop(['30_pct','real_price_30min','30_pct_max','real_price_30max'],axis=1,inplace=True)

        df_all.drop(['change','vol'],axis=1,inplace=True)

        #===================================================================================================================================#
    
        #明日幅度
        #tm1=df_all.groupby('ts_code')['pct_chg'].shift(-1)
        #tm2=df_all.groupby('ts_code')['pct_chg'].shift(-2)
        #tm3=df_all.groupby('ts_code')['pct_chg'].shift(-3)
        #df_all['tomorrow_chg']=((100+tm1)*(100+tm2)*(100+tm3)-1000000)/10000
        df_all['tomorrow_chg']=df_all.groupby('ts_code')['pct_chg'].shift(-1)
        #明日排名
        df_all['tomorrow_chg_rank']=df_all.groupby('trade_date')['tomorrow_chg'].rank(pct=True)
        df_all['tomorrow_chg_rank']=df_all['tomorrow_chg_rank']*9.9//1
        #是否停
        df_all['high_stop']=0
        df_all.loc[df_all['pct_chg']>9,'high_stop']=1
        df_all.loc[(df_all['pct_chg']<5.4) & (4.6<df_all['pct_chg']),'high_stop']=1


        #真实价格范围(区分实际股价高低)
        df_all['price_real_rank']=df_all.groupby('trade_date')['pre_close'].rank(pct=True)
        df_all['price_real_rank']=df_all['price_real_rank']*10//1
        #1日
        df_all['chg_rank']=df_all.groupby('trade_date')['pct_chg'].rank(pct=True)
        df_all['chg_rank']=df_all['chg_rank']*10//1


        #6日
        xxx=df_all.groupby('ts_code')['chg_rank'].rolling(6).sum().reset_index()
        xxx.set_index(['level_1'], drop=True, append=False, inplace=True, verify_integrity=False)
        xxx.drop(['ts_code'],axis=1,inplace=True)

        df_all=df_all.join(xxx, lsuffix='', rsuffix='_6')

        df_all['chg_rank_6']=df_all.groupby('trade_date')['chg_rank_6'].rank(pct=True)
        df_all['chg_rank_6']=df_all['chg_rank_6']*10//1

        #10日
        xxx=df_all.groupby('ts_code')['chg_rank'].rolling(10).sum().reset_index()
        xxx.set_index(['level_1'], drop=True, append=False, inplace=True, verify_integrity=False)
        xxx.drop(['ts_code'],axis=1,inplace=True)

        df_all=df_all.join(xxx, lsuffix='', rsuffix='_10')

        df_all['chg_rank_10']=df_all.groupby('trade_date')['chg_rank_10'].rank(pct=True)
        df_all['chg_rank_10']=df_all['chg_rank_10']*10//1

        #3日
        xxx=df_all.groupby('ts_code')['chg_rank'].rolling(3).sum().reset_index()
        xxx.set_index(['level_1'], drop=True, append=False, inplace=True, verify_integrity=False)
        xxx.drop(['ts_code'],axis=1,inplace=True)

        df_all=df_all.join(xxx, lsuffix='', rsuffix='_3')

        df_all['chg_rank_3']=df_all.groupby('trade_date')['chg_rank_3'].rank(pct=True)
        df_all['chg_rank_3']=df_all['chg_rank_3']*10//1

        #print(df_all)

        #10日均量
        xxx=df_all.groupby('ts_code')['amount'].rolling(10).mean().reset_index()
        xxx.set_index(['level_1'], drop=True, append=False, inplace=True, verify_integrity=False)
        xxx.drop(['ts_code'],axis=1,inplace=True)
        df_all=df_all.join(xxx, lsuffix='_1', rsuffix='_10')

        #当日量占比
        df_all['pst_amount']=df_all['amount_1']/df_all['amount_10']
        df_all.drop(['amount_1','amount_10'],axis=1,inplace=True)
        #当日量排名
        df_all['pst_amount_rank']=df_all.groupby('trade_date')['pst_amount'].rank(pct=True)
        df_all['pst_amount_rank']=df_all['pst_amount_rank']*10//1

        #计算三种比例rank
        dolist=['open','high','low']

        for curc in dolist:
            buffer=((df_all[curc]-df_all['pre_close'])*100)/df_all['close']
            df_all[curc]=buffer
            df_all[curc]=df_all.groupby('trade_date')[curc].rank(pct=True)
            df_all[curc]=df_all[curc]*10//1

        #加入昨日rank
        df_all['yesterday_open']=df_all.groupby('ts_code')['open'].shift(1)
        df_all['yesterday_high']=df_all.groupby('ts_code')['high'].shift(1)
        df_all['yesterday_low']=df_all.groupby('ts_code')['low'].shift(1)
        df_all['yesterday_pst_amount_rank']=df_all.groupby('ts_code')['pst_amount_rank'].shift(1)
        #加入前日open
        df_all['yesterday2_open']=df_all.groupby('ts_code')['open'].shift(2)

        df_all.drop(['close','pre_close','pct_chg','pst_amount','adj_factor','real_price'],axis=1,inplace=True)
        #暂时不用的列
        df_all=df_all[df_all['high_stop']==0]
        #'tomorrow_chg'
        df_all.drop(['high_stop'],axis=1,inplace=True) 

        df_all.dropna(axis=0,how='any',inplace=True)

        print(df_all)
        df_all=df_all.reset_index(drop=True)

        return df_all

    def real_FE(self,gap_day):

        df_data=pd.read_csv('real_now.csv',index_col=0,header=0)
        df_adj_all=pd.read_csv('real_adj_now.csv',index_col=0,header=0)

        df_all=pd.merge(df_data, df_adj_all, how='left', on=['ts_code','trade_date'])

        #加入间隔
        df_all['gap_day']=gap_day

        #df_all=pd.read_csv(bufferstring,index_col=0,header=0,nrows=100000)
    
        #df_all.drop(['change','vol'],axis=1,inplace=True)
 

        #===================================================================================================================================#

        #复权后价格
        df_all['adj_factor']=df_all['adj_factor'].fillna(0)
        df_all['real_price']=df_all['close']*df_all['adj_factor']
        
        df_all['real_price']=df_all.groupby('ts_code')['real_price'].shift(1)
        df_all['real_price']=df_all['real_price']*(1+df_all['pct_chg']/100)


        #30日最低比值
        xxx=df_all.groupby('ts_code')['real_price'].rolling(30).min().reset_index()
        xxx.set_index(['level_1'], drop=True, append=False, inplace=True, verify_integrity=False)
        xxx.drop(['ts_code'],axis=1,inplace=True)
        
        df_all=df_all.join(xxx, lsuffix='', rsuffix='_30min')
        #bbb=df_all.groupby('ts_code')['real_price'].agg({'all_min':np.min, 'all_max': np.max}).reset_index()
        #ccc=pd.merge(df_all, bbb, how='inner', on=['ts_code'])
        df_all['30_pct']=(df_all['real_price']-df_all['real_price_30min'])/df_all['real_price_30min']
        df_all['30_pct_rank']=df_all.groupby('trade_date')['30_pct'].rank(pct=True)
        df_all['30_pct_rank']=df_all['30_pct_rank']*10//1

        #30日最高比值
        xxx=df_all.groupby('ts_code')['real_price'].rolling(30).max().reset_index()
        xxx.set_index(['level_1'], drop=True, append=False, inplace=True, verify_integrity=False)
        xxx.drop(['ts_code'],axis=1,inplace=True)
        
        df_all=df_all.join(xxx, lsuffix='', rsuffix='_30max')
        #bbb=df_all.groupby('ts_code')['real_price'].agg({'all_min':np.min, 'all_max': np.max}).reset_index()
        #ccc=pd.merge(df_all, bbb, how='inner', on=['ts_code'])
        df_all['30_pct_max']=(df_all['real_price']-df_all['real_price_30max'])/df_all['real_price_30max']
        df_all['30_pct_max_rank']=df_all.groupby('trade_date')['30_pct_max'].rank(pct=True)
        df_all['30_pct_max_rank']=df_all['30_pct_max_rank']*10//1

        df_all.drop(['30_pct','real_price_30min','30_pct_max','real_price_30max'],axis=1,inplace=True)


        #===================================================================================================================================#


        #是否停
        df_all['high_stop']=0
        df_all.loc[df_all['pct_chg']>9,'high_stop']=1
        df_all.loc[(df_all['pct_chg']<5.5) & (4.5<df_all['pct_chg']),'high_stop']=1


        #真实价格范围
        df_all['price_real_rank']=df_all.groupby('trade_date')['pre_close'].rank(pct=True)
        df_all['price_real_rank']=df_all['price_real_rank']*10//1
        #1日
        df_all['chg_rank']=df_all.groupby('trade_date')['pct_chg'].rank(pct=True)
        df_all['chg_rank']=df_all['chg_rank']*10//1


        #6日
        xxx=df_all.groupby('ts_code')['chg_rank'].rolling(6).sum().reset_index()
        xxx.set_index(['level_1'], drop=True, append=False, inplace=True, verify_integrity=False)
        xxx.drop(['ts_code'],axis=1,inplace=True)

        df_all=df_all.join(xxx, lsuffix='', rsuffix='_6')

        df_all['chg_rank_6']=df_all.groupby('trade_date')['chg_rank_6'].rank(pct=True)
        df_all['chg_rank_6']=df_all['chg_rank_6']*10//1

        #10日
        xxx=df_all.groupby('ts_code')['chg_rank'].rolling(10).sum().reset_index()
        xxx.set_index(['level_1'], drop=True, append=False, inplace=True, verify_integrity=False)
        xxx.drop(['ts_code'],axis=1,inplace=True)

        df_all=df_all.join(xxx, lsuffix='', rsuffix='_10')

        df_all['chg_rank_10']=df_all.groupby('trade_date')['chg_rank_10'].rank(pct=True)
        df_all['chg_rank_10']=df_all['chg_rank_10']*10//1

        #3日
        xxx=df_all.groupby('ts_code')['chg_rank'].rolling(3).sum().reset_index()
        xxx.set_index(['level_1'], drop=True, append=False, inplace=True, verify_integrity=False)
        xxx.drop(['ts_code'],axis=1,inplace=True)

        df_all=df_all.join(xxx, lsuffix='', rsuffix='_3')

        df_all['chg_rank_3']=df_all.groupby('trade_date')['chg_rank_3'].rank(pct=True)
        df_all['chg_rank_3']=df_all['chg_rank_3']*10//1

        #print(df_all)

        #10日均量
        xxx=df_all.groupby('ts_code')['amount'].rolling(10).mean().reset_index()
        xxx.set_index(['level_1'], drop=True, append=False, inplace=True, verify_integrity=False)
        xxx.drop(['ts_code'],axis=1,inplace=True)
        df_all=df_all.join(xxx, lsuffix='_1', rsuffix='_10')

        #当日量占比
        df_all['pst_amount']=df_all['amount_1']/df_all['amount_10']
        df_all.drop(['amount_1','amount_10'],axis=1,inplace=True)
        #当日量排名
        df_all['pst_amount_rank']=df_all.groupby('trade_date')['pst_amount'].rank(pct=True)
        df_all['pst_amount_rank']=df_all['pst_amount_rank']*10//1

        #计算三种比例rank
        dolist=['open','high','low']

        for curc in dolist:
            buffer=((df_all[curc]-df_all['pre_close'])*100)/df_all['pre_close']
            df_all[curc]=buffer
            df_all[curc]=df_all.groupby('trade_date')[curc].rank(pct=True)
            df_all[curc]=df_all[curc]*10//1

        #加入昨日rank
        df_all['yesterday_open']=df_all.groupby('ts_code')['open'].shift(1)
        df_all['yesterday_high']=df_all.groupby('ts_code')['high'].shift(1)
        df_all['yesterday_low']=df_all.groupby('ts_code')['low'].shift(1)
        df_all['yesterday_pst_amount_rank']=df_all.groupby('ts_code')['pst_amount_rank'].shift(1)
        #加入前日open
        df_all['yesterday2_open']=df_all.groupby('ts_code')['open'].shift(2)

        df_all.drop(['close','pre_close','pct_chg','pst_amount','adj_factor','real_price'],axis=1,inplace=True)
        #暂时不用的列
        df_all=df_all[df_all['high_stop']==0]
        #'tomorrow_chg'
        df_all.drop(['high_stop'],axis=1,inplace=True)



        df_all.dropna(axis=0,how='any',inplace=True)


        month_sec=df_all['trade_date'].max()
        df_all=df_all[df_all['trade_date']!=month_sec]
        print(df_all)
        df_all=df_all.reset_index(drop=True)

        df_all.to_csv('today_train.csv')
        dwdw=1

    def data_feature(self,DataSetName):


        df_all=pd.read_csv(DataSetName,index_col=0,header=0)    
        
        start=df_all['trade_date'].apply(str)[0]
        end=df_all['trade_date'].apply(str)[df_all.shape[0]-1]
        xxx=pd.date_range(start,end)

        df = pd.DataFrame(xxx)
        df.columns = ['trade_date']
        df['trade_date']=df['trade_date'].map(str).map(lambda x : x[:4]+x[5:7]+x[8:10]).astype("int64")

        yyy=df_all['trade_date']
        zzz2=yyy.unique()
        df_2=pd.DataFrame(zzz2)
        df_2.columns = ['trade_date']
        df_2['day_flag']=1
    
        result = pd.merge(df, df_2, how='left', on=['trade_date'])
        result['day_flag2']=result['day_flag'].shift(-1)
        result['gap_day']=0

        result.loc[(result['day_flag']==1) & (result['day_flag2']!=1),'gap_day']=1

        result.drop(['day_flag','day_flag2'],axis=1,inplace=True)

        df_all=pd.merge(df_all, result, how='left', on=['trade_date'])

        #print(df_all)

        df_all.to_csv('datatest.csv')

        dsfsef=1

class FE7(FEbase):
    def __init__(self):
        pass
    def changerank(self,a,b):
        #a.loc[0.94<=a[b],b]=8      
        #a.loc[(a[b]<0.16) & (0.06<=a[b]),b]=1
        #a.loc[(a[b]<0.3) & (0.16<=a[b]),b]=2
        #a.loc[(a[b]<0.40) & (0.3<=a[b]),b]=3
        #a.loc[(a[b]<0.60) & (0.40<=a[b]),b]=4
        #a.loc[(a[b]<0.7) & (0.60<=a[b]),b]=5
        #a.loc[(a[b]<0.84) & (0.7<=a[b]),b]=6
        #a.loc[(a[b]<0.94) & (0.84<=a[b]),b]=7
        #a.loc[(a[b]<0.06) ,b]=0

        a[b]=a[b]*10//1
        return a[b]
    def changerank2(self,a,b):
        #a.loc[0.94<=a[b],b]=9   
        #a.loc[(a[b]<0.16) & (0.06<=a[b]),b]=1
        #a.loc[(a[b]<0.3) & (0.16<=a[b]),b]=2
        #a.loc[(a[b]<0.40) & (0.3<=a[b]),b]=3
        #a.loc[(a[b]<0.50) & (0.40<=a[b]),b]=4
        #a.loc[(a[b]<0.60) & (0.50<=a[b]),b]=5
        #a.loc[(a[b]<0.7) & (0.60<=a[b]),b]=6
        #a.loc[(a[b]<0.84) & (0.7<=a[b]),b]=7
        #a.loc[(a[b]<0.94) & (0.84<=a[b]),b]=8
        #a.loc[(a[b]<0.06) ,b]=0

        a[b]=a[b]*10//1
        return a[b]
    def core(self,DataSetName):

        df_data=pd.read_csv(DataSetName[0],index_col=0,header=0)
        df_adj_all=pd.read_csv(DataSetName[1],index_col=0,header=0)
        #df_long_all=pd.read_csv(DataSetName[2],index_col=0,header=0)
        df_moneyflow_all=pd.read_csv(DataSetName[2],index_col=0,header=0)

        df_all=pd.merge(df_data, df_adj_all, how='inner', on=['ts_code','trade_date'])
        #df_all=pd.merge(df_all, df_long_all, how='inner', on=['ts_code','trade_date'])
        df_all=pd.merge(df_all, df_moneyflow_all, how='inner', on=['ts_code','trade_date'])

        #print(df_all)

        #===================================================================================================================================#
        ##买入卖出额在所有股票中的占比(先假设可以看当日)
        ##1日
        #df_all['moneyflow_rank']=df_all.groupby('trade_date')['net_mf_amount'].rank(pct=True)
        #df_all['moneyflow_rank']=self.changerank(df_all,'moneyflow_rank')

        ##3日
        #xxx=df_all.groupby('ts_code')['moneyflow_rank'].rolling(3).sum().reset_index()
        #xxx.set_index(['level_1'], drop=True, append=False, inplace=True, verify_integrity=False)
        #xxx.drop(['ts_code'],axis=1,inplace=True)

        #df_all=df_all.join(xxx, lsuffix='', rsuffix='_3')

        #df_all['moneyflow_rank_3']=df_all.groupby('trade_date')['moneyflow_rank_3'].rank(pct=True)
        #df_all['moneyflow_rank_3']=self.changerank(df_all,'moneyflow_rank_3')

        #买入卖出的大单占比排行

        #10日均量
        xxx=df_all.groupby('ts_code')['buy_lg_amount'].rolling(10).mean().reset_index()
        xxx.set_index(['level_1'], drop=True, append=False, inplace=True, verify_integrity=False)
        xxx.drop(['ts_code'],axis=1,inplace=True)
        df_all=df_all.join(xxx, lsuffix='_1', rsuffix='_10')

        #当日量占比
        df_all['pst_buy_lg']=df_all['buy_lg_amount_1']/df_all['buy_lg_amount_10']
        df_all.drop(['buy_lg_amount_1','buy_lg_amount_10'],axis=1,inplace=True)

        #当日量排名
        df_all['pst_buy_lg_rank']=df_all.groupby('trade_date')['pst_buy_lg'].rank(pct=True)
        df_all['pst_buy_lg_rank']=self.changerank(df_all,'pst_buy_lg_rank')



        #===================================================================================================================================#
        #df_all['pe'] = df_all['pe'].fillna(9999)
        #df_all['pb'] = df_all['pb'].fillna(9999)

        #df_all['pe_rank']=df_all.groupby('trade_date')['pe'].rank(pct=True)
        #df_all['pb_rank']=df_all.groupby('trade_date')['pb'].rank(pct=True)        
        #df_all['pe_rank']=df_all['pe_rank']*10//1
        #df_all['pb_rank']=df_all['pb_rank']*10//1

        #df_all.drop(['turnover_rate','volume_ratio','pe','pb'],axis=1,inplace=True)

        #print(df_all)
        #df_all.to_csv('sjefosia.csv')

        #===================================================================================================================================#

        #加入gap_day特征
        start=df_all['trade_date'].apply(str)[0]
        end=df_all['trade_date'].apply(str)[df_all.shape[0]-1]
        xxx=pd.date_range(start,end)

        df = pd.DataFrame(xxx)
        df.columns = ['trade_date']
        df['trade_date']=df['trade_date'].map(str).map(lambda x : x[:4]+x[5:7]+x[8:10]).astype("int64")

        yyy=df_all['trade_date']
        zzz2=yyy.unique()
        df_2=pd.DataFrame(zzz2)
        df_2.columns = ['trade_date']
        df_2['day_flag']=1
    
        result = pd.merge(df, df_2, how='left', on=['trade_date'])
        result['day_flag2']=result['day_flag'].shift(-1)
        result['gap_day']=0

        result.loc[(result['day_flag']==1) & (result['day_flag2']!=1),'gap_day']=1

        result.drop(['day_flag','day_flag2'],axis=1,inplace=True)

        df_all=pd.merge(df_all, result, how='left', on=['trade_date'])

        #===================================================================================================================================#

        #复权后价格
        df_all['real_price']=df_all['close']*df_all['adj_factor']
        
        #30日最低比值
        xxx=df_all.groupby('ts_code')['real_price'].rolling(30).min().reset_index()
        xxx.set_index(['level_1'], drop=True, append=False, inplace=True, verify_integrity=False)
        xxx.drop(['ts_code'],axis=1,inplace=True)
        
        df_all=df_all.join(xxx, lsuffix='', rsuffix='_30min')
        #bbb=df_all.groupby('ts_code')['real_price'].agg({'all_min':np.min, 'all_max': np.max}).reset_index()
        #ccc=pd.merge(df_all, bbb, how='inner', on=['ts_code'])
        df_all['30_pct']=(df_all['real_price']-df_all['real_price_30min'])/df_all['real_price_30min']
        df_all['30_pct_rank']=df_all.groupby('trade_date')['30_pct'].rank(pct=True)
        df_all['30_pct_rank']=self.changerank(df_all,'30_pct_rank')
        #df_all['30_pct_rank']=df_all['30_pct_rank']*4.99//1

        #30日最高比值
        xxx=df_all.groupby('ts_code')['real_price'].rolling(30).max().reset_index()
        xxx.set_index(['level_1'], drop=True, append=False, inplace=True, verify_integrity=False)
        xxx.drop(['ts_code'],axis=1,inplace=True)
        
        df_all=df_all.join(xxx, lsuffix='', rsuffix='_30max')
        #bbb=df_all.groupby('ts_code')['real_price'].agg({'all_min':np.min, 'all_max': np.max}).reset_index()
        #ccc=pd.merge(df_all, bbb, how='inner', on=['ts_code'])
        df_all['30_pct_max']=(df_all['real_price']-df_all['real_price_30max'])/df_all['real_price_30max']
        df_all['30_pct_max_rank']=df_all.groupby('trade_date')['30_pct_max'].rank(pct=True)
        df_all['30_pct_max_rank']=self.changerank(df_all,'30_pct_max_rank')

        df_all.drop(['30_pct','real_price_30min','30_pct_max','real_price_30max'],axis=1,inplace=True)

        df_all.drop(['change','vol'],axis=1,inplace=True)

        #===================================================================================================================================#
    
        #明日幅度
        #tm1=df_all.groupby('ts_code')['pct_chg'].shift(-1)
        #tm2=df_all.groupby('ts_code')['pct_chg'].shift(-2)
        #tm3=df_all.groupby('ts_code')['pct_chg'].shift(-3)
        #df_all['tomorrow_chg']=((100+tm1)*(100+tm2)*(100+tm3)-1000000)/10000
        df_all['tomorrow_chg']=df_all.groupby('ts_code')['pct_chg'].shift(-1)
        #明日排名
        df_all['tomorrow_chg_rank']=df_all.groupby('trade_date')['tomorrow_chg'].rank(pct=True)
        #df_all['tomorrow_chg_rank']=df_all['tomorrow_chg_rank']*9.99//1
        df_all['tomorrow_chg_rank']=self.changerank2(df_all,'tomorrow_chg_rank');
        #是否停
        df_all['high_stop']=0
        df_all.loc[df_all['pct_chg']>9,'high_stop']=1
        df_all.loc[(df_all['pct_chg']<5.4) & (4.6<df_all['pct_chg']),'high_stop']=1

        #===================================================================================================================================#

        #真实价格范围(区分实际股价高低)
        df_all['price_real_rank']=df_all.groupby('trade_date')['pre_close'].rank(pct=True)
        df_all['price_real_rank']=self.changerank(df_all,'price_real_rank')

        #===================================================================================================================================#
        #K线相关
        #1日
        df_all['chg_rank']=df_all.groupby('trade_date')['pct_chg'].rank(pct=True)
        df_all['chg_rank']=self.changerank(df_all,'chg_rank')

        #6日
        xxx=df_all.groupby('ts_code')['chg_rank'].rolling(6).sum().reset_index()
        xxx.set_index(['level_1'], drop=True, append=False, inplace=True, verify_integrity=False)
        xxx.drop(['ts_code'],axis=1,inplace=True)

        df_all=df_all.join(xxx, lsuffix='', rsuffix='_6')

        df_all['chg_rank_6']=df_all.groupby('trade_date')['chg_rank_6'].rank(pct=True)
        df_all['chg_rank_6']=self.changerank(df_all,'chg_rank_6')

        #10日
        xxx=df_all.groupby('ts_code')['chg_rank'].rolling(10).sum().reset_index()
        xxx.set_index(['level_1'], drop=True, append=False, inplace=True, verify_integrity=False)
        xxx.drop(['ts_code'],axis=1,inplace=True)

        df_all=df_all.join(xxx, lsuffix='', rsuffix='_10')

        df_all['chg_rank_10']=df_all.groupby('trade_date')['chg_rank_10'].rank(pct=True)
        df_all['chg_rank_10']=self.changerank(df_all,'chg_rank_10')

        #3日
        xxx=df_all.groupby('ts_code')['chg_rank'].rolling(3).sum().reset_index()
        xxx.set_index(['level_1'], drop=True, append=False, inplace=True, verify_integrity=False)
        xxx.drop(['ts_code'],axis=1,inplace=True)

        df_all=df_all.join(xxx, lsuffix='', rsuffix='_3')

        df_all['chg_rank_3']=df_all.groupby('trade_date')['chg_rank_3'].rank(pct=True)
        df_all['chg_rank_3']=self.changerank(df_all,'chg_rank_3')


        #===================================================================================================================================#

        ##20日涨停数排序
        #xxx=df_all.groupby('ts_code')['high_stop'].rolling(20).sum().reset_index()
        #xxx.set_index(['level_1'], drop=True, append=False, inplace=True, verify_integrity=False)
        #xxx.drop(['ts_code'],axis=1,inplace=True)
        #df_all=df_all.join(xxx, lsuffix='', rsuffix='_20')
        #df_all['high_stop_20']=self.changerank(df_all,'high_stop_20')

        #print(df_all)

        #===================================================================================================================================#
        #量相关

        #10日均量
        xxx=df_all.groupby('ts_code')['amount'].rolling(10).mean().reset_index()
        xxx.set_index(['level_1'], drop=True, append=False, inplace=True, verify_integrity=False)
        xxx.drop(['ts_code'],axis=1,inplace=True)
        df_all=df_all.join(xxx, lsuffix='_1', rsuffix='_10')

        #当日量占比
        df_all['pst_amount']=df_all['amount_1']/df_all['amount_10']
        df_all.drop(['amount_1','amount_10'],axis=1,inplace=True)
        #当日量排名
        df_all['pst_amount_rank']=df_all.groupby('trade_date')['pst_amount'].rank(pct=True)
        df_all['pst_amount_rank']=self.changerank(df_all,'pst_amount_rank')

        #计算三种比例rank
        dolist=['open','high','low']

        for curc in dolist:
            buffer=((df_all[curc]-df_all['pre_close'])*100)/df_all['close']
            df_all[curc]=buffer
            df_all[curc]=df_all.groupby('trade_date')[curc].rank(pct=True)
            df_all[curc]=self.changerank(df_all,curc)

        #加入昨日rank
        df_all['yesterday_open']=df_all.groupby('ts_code')['open'].shift(1)
        df_all['yesterday_high']=df_all.groupby('ts_code')['high'].shift(1)
        df_all['yesterday_low']=df_all.groupby('ts_code')['low'].shift(1)
        df_all['yesterday_pst_amount_rank']=df_all.groupby('ts_code')['pst_amount_rank'].shift(1)
        #加入前日open
        df_all['yesterday2_open']=df_all.groupby('ts_code')['open'].shift(2)

        df_all.drop(['close','pre_close','pct_chg','pst_amount','adj_factor','real_price'],axis=1,inplace=True)
        #暂时不用的列
        df_all=df_all[df_all['high_stop']==0]
        #'tomorrow_chg'
        df_all.drop(['high_stop'],axis=1,inplace=True)
        
        

        df_all.dropna(axis=0,how='any',inplace=True)

        df_all.drop(['buy_sm_vol','buy_sm_amount','sell_sm_vol',
                     'sell_sm_amount','buy_md_vol','buy_md_amount','sell_md_vol','sell_md_amount',
                     'buy_lg_vol','sell_lg_vol','sell_lg_amount','buy_elg_vol','buy_elg_amount',
                     'sell_elg_vol','sell_elg_amount','net_mf_vol','net_mf_amount','pst_buy_lg'],axis=1,inplace=True)

        print(df_all)
        #df_test=df_all.reset_index(drop=True)

        #                #循环非1的列
        #for usecol in df_test.columns.tolist()[1:]:
        #    if(usecol not in["ts_code","trade_date","tomorrow_chg","tomorrow_chg_rank"] ):
        #        #将当前列全部转为字符类型
        #        df_test[usecol] = df_test[usecol].astype('int8')
            
        #        ##Fit LabelEncoder
        #        ##对训练集和测试集同时做labelencoder
        #        #le = LabelEncoder().fit(np.unique(df_test[usecol].unique().tolist()))

        #        ##At the end 0 will be used for dropped values
        #        ##同上英文翻译
        #        #df_test[usecol] = le.transform(df_test[usecol])+1

        #        #            #以当前关键字从大到小排列相当于每个labelencoder的数字代表了他出现的频繁度(?)
        #        #            #动原始行前先复制一份出来
        #        #agg[usecol+'Copy'] = agg[usecol]

        #        #df_test[usecol] = (pd.merge(df_test[[usecol]], 
        #        #                          agg[[usecol, usecol+'Copy']], 
        #        #                          on=usecol, how='left')[usecol+'Copy']
        #        #                 .replace(np.nan, 0).astype('int').astype('category'))
        #        df_test=self.one_hot(df_test,usecol)
        #        sfdasf=1

        return df_all
        #return df_test



    def real_FE(self,gap_day):

        df_data=pd.read_csv('real_now.csv',index_col=0,header=0)
        df_adj_all=pd.read_csv('real_adj_now.csv',index_col=0,header=0)

        df_all=pd.merge(df_data, df_adj_all, how='left', on=['ts_code','trade_date'])

        #加入间隔
        df_all['gap_day']=gap_day

        #df_all=pd.read_csv(bufferstring,index_col=0,header=0,nrows=100000)
    
        #df_all.drop(['change','vol'],axis=1,inplace=True)
 

        #===================================================================================================================================#

        #复权后价格
        df_all['adj_factor']=df_all['adj_factor'].fillna(0)
        df_all['real_price']=df_all['close']*df_all['adj_factor']
        
        df_all['real_price']=df_all.groupby('ts_code')['real_price'].shift(1)
        df_all['real_price']=df_all['real_price']*(1+df_all['pct_chg']/100)


        #30日最低比值
        xxx=df_all.groupby('ts_code')['real_price'].rolling(30).min().reset_index()
        xxx.set_index(['level_1'], drop=True, append=False, inplace=True, verify_integrity=False)
        xxx.drop(['ts_code'],axis=1,inplace=True)
        
        df_all=df_all.join(xxx, lsuffix='', rsuffix='_30min')
        #bbb=df_all.groupby('ts_code')['real_price'].agg({'all_min':np.min, 'all_max': np.max}).reset_index()
        #ccc=pd.merge(df_all, bbb, how='inner', on=['ts_code'])
        df_all['30_pct']=(df_all['real_price']-df_all['real_price_30min'])/df_all['real_price_30min']
        df_all['30_pct_rank']=df_all.groupby('trade_date')['30_pct'].rank(pct=True)
        df_all['30_pct_rank']=df_all['30_pct_rank']*10//1

        #30日最高比值
        xxx=df_all.groupby('ts_code')['real_price'].rolling(30).max().reset_index()
        xxx.set_index(['level_1'], drop=True, append=False, inplace=True, verify_integrity=False)
        xxx.drop(['ts_code'],axis=1,inplace=True)
        
        df_all=df_all.join(xxx, lsuffix='', rsuffix='_30max')
        #bbb=df_all.groupby('ts_code')['real_price'].agg({'all_min':np.min, 'all_max': np.max}).reset_index()
        #ccc=pd.merge(df_all, bbb, how='inner', on=['ts_code'])
        df_all['30_pct_max']=(df_all['real_price']-df_all['real_price_30max'])/df_all['real_price_30max']
        df_all['30_pct_max_rank']=df_all.groupby('trade_date')['30_pct_max'].rank(pct=True)
        df_all['30_pct_max_rank']=df_all['30_pct_max_rank']*10//1

        df_all.drop(['30_pct','real_price_30min','30_pct_max','real_price_30max'],axis=1,inplace=True)


        #===================================================================================================================================#


        #是否停
        df_all['high_stop']=0
        df_all.loc[df_all['pct_chg']>9,'high_stop']=1
        df_all.loc[(df_all['pct_chg']<5.5) & (4.5<df_all['pct_chg']),'high_stop']=1


        #真实价格范围
        df_all['price_real_rank']=df_all.groupby('trade_date')['pre_close'].rank(pct=True)
        df_all['price_real_rank']=df_all['price_real_rank']*10//1
        #1日
        df_all['chg_rank']=df_all.groupby('trade_date')['pct_chg'].rank(pct=True)
        df_all['chg_rank']=df_all['chg_rank']*10//1


        #6日
        xxx=df_all.groupby('ts_code')['chg_rank'].rolling(6).sum().reset_index()
        xxx.set_index(['level_1'], drop=True, append=False, inplace=True, verify_integrity=False)
        xxx.drop(['ts_code'],axis=1,inplace=True)

        df_all=df_all.join(xxx, lsuffix='', rsuffix='_6')

        df_all['chg_rank_6']=df_all.groupby('trade_date')['chg_rank_6'].rank(pct=True)
        df_all['chg_rank_6']=df_all['chg_rank_6']*10//1

        #10日
        xxx=df_all.groupby('ts_code')['chg_rank'].rolling(10).sum().reset_index()
        xxx.set_index(['level_1'], drop=True, append=False, inplace=True, verify_integrity=False)
        xxx.drop(['ts_code'],axis=1,inplace=True)

        df_all=df_all.join(xxx, lsuffix='', rsuffix='_10')

        df_all['chg_rank_10']=df_all.groupby('trade_date')['chg_rank_10'].rank(pct=True)
        df_all['chg_rank_10']=df_all['chg_rank_10']*10//1

        #3日
        xxx=df_all.groupby('ts_code')['chg_rank'].rolling(3).sum().reset_index()
        xxx.set_index(['level_1'], drop=True, append=False, inplace=True, verify_integrity=False)
        xxx.drop(['ts_code'],axis=1,inplace=True)

        df_all=df_all.join(xxx, lsuffix='', rsuffix='_3')

        df_all['chg_rank_3']=df_all.groupby('trade_date')['chg_rank_3'].rank(pct=True)
        df_all['chg_rank_3']=df_all['chg_rank_3']*10//1

        #print(df_all)

        #10日均量
        xxx=df_all.groupby('ts_code')['amount'].rolling(10).mean().reset_index()
        xxx.set_index(['level_1'], drop=True, append=False, inplace=True, verify_integrity=False)
        xxx.drop(['ts_code'],axis=1,inplace=True)
        df_all=df_all.join(xxx, lsuffix='_1', rsuffix='_10')

        #当日量占比
        df_all['pst_amount']=df_all['amount_1']/df_all['amount_10']
        df_all.drop(['amount_1','amount_10'],axis=1,inplace=True)
        #当日量排名
        df_all['pst_amount_rank']=df_all.groupby('trade_date')['pst_amount'].rank(pct=True)
        df_all['pst_amount_rank']=df_all['pst_amount_rank']*10//1

        #计算三种比例rank
        dolist=['open','high','low']

        for curc in dolist:
            buffer=((df_all[curc]-df_all['pre_close'])*100)/df_all['pre_close']
            df_all[curc]=buffer
            df_all[curc]=df_all.groupby('trade_date')[curc].rank(pct=True)
            df_all[curc]=df_all[curc]*10//1

        #加入昨日rank
        df_all['yesterday_open']=df_all.groupby('ts_code')['open'].shift(1)
        df_all['yesterday_high']=df_all.groupby('ts_code')['high'].shift(1)
        df_all['yesterday_low']=df_all.groupby('ts_code')['low'].shift(1)
        df_all['yesterday_pst_amount_rank']=df_all.groupby('ts_code')['pst_amount_rank'].shift(1)
        #加入前日open
        df_all['yesterday2_open']=df_all.groupby('ts_code')['open'].shift(2)

        df_all.drop(['close','pre_close','pct_chg','pst_amount','adj_factor','real_price'],axis=1,inplace=True)
        #暂时不用的列
        df_all=df_all[df_all['high_stop']==0]
        #'tomorrow_chg'
        df_all.drop(['high_stop'],axis=1,inplace=True)



        df_all.dropna(axis=0,how='any',inplace=True)


        month_sec=df_all['trade_date'].max()
        df_all=df_all[df_all['trade_date']!=month_sec]
        print(df_all)
        df_all=df_all.reset_index(drop=True)

        df_all.to_csv('today_train.csv')
        dwdw=1
    def one_hot(self,dataset,column_name):

        dummies = pd.get_dummies(dataset[column_name], prefix= column_name,prefix_sep='__')
        dataset.drop([column_name], axis=1, inplace=True)    
        dataset=dataset.join(dummies)
        return dataset

        dsfsfe=1
    def new_test(self,savepath):
        df_test=pd.read_csv('fefortest.csv',index_col=0,header=0)

        print(df_test)
                #循环非1的列
        for usecol in df_test.columns.tolist()[1:]:
            if(usecol not in["ts_code","trade_date","tomorrow_chg","tomorrow_chg_rank"] ):
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
                df_test=self.one_hot(df_test,usecol)
                sfdasf=1

        #print(df_test)
        df_test.to_csv(savepath)
        asdfaf=1

class FEb(FEbase):
    def __init__(self):
        pass

    def core(self,DataSetName):

        df_data=pd.read_csv(DataSetName[0],index_col=0,header=0)
        df_adj_all=pd.read_csv(DataSetName[1],index_col=0,header=0)
        #df_long_all=pd.read_csv(DataSetName[2],index_col=0,header=0)

        df_all=pd.merge(df_data, df_adj_all, how='inner', on=['ts_code','trade_date'])
        #df_all=pd.merge(df_all, df_long_all, how='inner', on=['ts_code','trade_date'])

        #===================================================================================================================================#
        #df_all['pe'] = df_all['pe'].fillna(9999)
        #df_all['pb'] = df_all['pb'].fillna(9999)

        #df_all['pe_rank']=df_all.groupby('trade_date')['pe'].rank(pct=True)
        #df_all['pb_rank']=df_all.groupby('trade_date')['pb'].rank(pct=True)        
        #df_all['pe_rank']=df_all['pe_rank']*10//1
        #df_all['pb_rank']=df_all['pb_rank']*10//1

        #df_all.drop(['turnover_rate','volume_ratio','pe','pb'],axis=1,inplace=True)

        #print(df_all)
        #df_all.to_csv('sjefosia.csv')

        #===================================================================================================================================#

        ##排除科创版
        #print(df_all)
        df_all=df_all[df_all['ts_code'].str.startswith('688')==False]
        

        #加入gap_day特征
        start=df_all['trade_date'].apply(str)[0]
        end=df_all['trade_date'].apply(str)[df_all.shape[0]-1]
        xxx=pd.date_range(start,end)

        df = pd.DataFrame(xxx)
        df.columns = ['trade_date']
        df['trade_date']=df['trade_date'].map(str).map(lambda x : x[:4]+x[5:7]+x[8:10]).astype("int64")

        yyy=df_all['trade_date']
        zzz2=yyy.unique()
        df_2=pd.DataFrame(zzz2)
        df_2.columns = ['trade_date']
        df_2['day_flag']=1
    
        result = pd.merge(df, df_2, how='left', on=['trade_date'])
        result['day_flag2']=result['day_flag'].shift(-1)
        result['gap_day']=0

        result.loc[(result['day_flag']==1) & (result['day_flag2']!=1),'gap_day']=1

        result.drop(['day_flag','day_flag2'],axis=1,inplace=True)

        df_all=pd.merge(df_all, result, how='left', on=['trade_date'])

        #===================================================================================================================================#

        #复权后价格
        df_all['real_price']=df_all['close']*df_all['adj_factor']
        
        #30日最低比值
        xxx=df_all.groupby('ts_code')['real_price'].rolling(30).min().reset_index()
        xxx.set_index(['level_1'], drop=True, append=False, inplace=True, verify_integrity=False)
        xxx.drop(['ts_code'],axis=1,inplace=True)
        
        df_all=df_all.join(xxx, lsuffix='', rsuffix='_30min')
        #bbb=df_all.groupby('ts_code')['real_price'].agg({'all_min':np.min, 'all_max': np.max}).reset_index()
        #ccc=pd.merge(df_all, bbb, how='inner', on=['ts_code'])
        df_all['30_pct']=(df_all['real_price']-df_all['real_price_30min'])/df_all['real_price_30min']
        df_all['30_pct_rank']=df_all.groupby('trade_date')['30_pct'].rank(pct=True)
        df_all['30_pct_rank']=df_all['30_pct_rank']*10//2

        #30日最高比值
        xxx=df_all.groupby('ts_code')['real_price'].rolling(30).max().reset_index()
        xxx.set_index(['level_1'], drop=True, append=False, inplace=True, verify_integrity=False)
        xxx.drop(['ts_code'],axis=1,inplace=True)
        
        df_all=df_all.join(xxx, lsuffix='', rsuffix='_30max')
        #bbb=df_all.groupby('ts_code')['real_price'].agg({'all_min':np.min, 'all_max': np.max}).reset_index()
        #ccc=pd.merge(df_all, bbb, how='inner', on=['ts_code'])
        df_all['30_pct_max']=(df_all['real_price']-df_all['real_price_30max'])/df_all['real_price_30max']
        df_all['30_pct_max_rank']=df_all.groupby('trade_date')['30_pct_max'].rank(pct=True)
        df_all['30_pct_max_rank']=df_all['30_pct_max_rank']*10//2

        df_all.drop(['30_pct','real_price_30min','30_pct_max','real_price_30max'],axis=1,inplace=True)

        df_all.drop(['change','vol'],axis=1,inplace=True)

        #===================================================================================================================================#
    
        #明日幅度
        tm1=df_all.groupby('ts_code')['pct_chg'].shift(-1)
        tm2=df_all.groupby('ts_code')['pct_chg'].shift(-2)
        #df_all['tomorrow_chg']=((100+tm1)*(100+tm2)-10000)/100
        tm3=df_all.groupby('ts_code')['pct_chg'].shift(-3)
        tm4=df_all.groupby('ts_code')['pct_chg'].shift(-4)
        tm5=df_all.groupby('ts_code')['pct_chg'].shift(-5)

        df_all['tomorrow_chg']=(((100+tm1)/100)*((100+tm2)/100)*((100+tm3)/100)*((100+tm4)/100)*((100+tm5)/100)-1)*100

        #df_all['tomorrow_chg']=((100+tm1)*(100+tm2)*(100+tm3)-1000000)/10000
        #df_all['tomorrow_chg']=df_all.groupby('ts_code')['pct_chg'].shift(-1)
        #明日排名
        df_all['tomorrow_chg_rank']=df_all.groupby('trade_date')['tomorrow_chg'].rank(pct=True)
        df_all['tomorrow_chg_rank']=df_all['tomorrow_chg_rank']*9.9//1
        #是否停
        df_all['high_stop']=0
        df_all.loc[df_all['pct_chg']>9.4,'high_stop']=1
        df_all.loc[(df_all['pct_chg']<5.2) & (4.8<df_all['pct_chg']),'high_stop']=1


        ##真实价格范围(区分实际股价高低)
        #df_all['price_real_rank']=df_all.groupby('trade_date')['pre_close'].rank(pct=True)
        #df_all['price_real_rank']=df_all['price_real_rank']*10//1
        #1日
        df_all['chg_rank']=df_all.groupby('trade_date')['pct_chg'].rank(pct=True)
        df_all['chg_rank']=df_all['chg_rank']*10//2


        #6日
        xxx=df_all.groupby('ts_code')['chg_rank'].rolling(6).sum().reset_index()
        xxx.set_index(['level_1'], drop=True, append=False, inplace=True, verify_integrity=False)
        xxx.drop(['ts_code'],axis=1,inplace=True)

        df_all=df_all.join(xxx, lsuffix='', rsuffix='_6')

        df_all['chg_rank_6']=df_all.groupby('trade_date')['chg_rank_6'].rank(pct=True)
        df_all['chg_rank_6']=df_all['chg_rank_6']*10//2

        #10日
        xxx=df_all.groupby('ts_code')['chg_rank'].rolling(10).sum().reset_index()
        xxx.set_index(['level_1'], drop=True, append=False, inplace=True, verify_integrity=False)
        xxx.drop(['ts_code'],axis=1,inplace=True)

        df_all=df_all.join(xxx, lsuffix='', rsuffix='_10')

        df_all['chg_rank_10']=df_all.groupby('trade_date')['chg_rank_10'].rank(pct=True)
        df_all['chg_rank_10']=df_all['chg_rank_10']*10//2

        #3日
        xxx=df_all.groupby('ts_code')['chg_rank'].rolling(3).sum().reset_index()
        xxx.set_index(['level_1'], drop=True, append=False, inplace=True, verify_integrity=False)
        xxx.drop(['ts_code'],axis=1,inplace=True)

        df_all=df_all.join(xxx, lsuffix='', rsuffix='_3')

        df_all['chg_rank_3']=df_all.groupby('trade_date')['chg_rank_3'].rank(pct=True)
        df_all['chg_rank_3']=df_all['chg_rank_3']*10//2

        ##20日
        #xxx=df_all.groupby('ts_code')['chg_rank'].rolling(20).sum().reset_index()
        #xxx.set_index(['level_1'], drop=True, append=False, inplace=True, verify_integrity=False)
        #xxx.drop(['ts_code'],axis=1,inplace=True)

        #df_all=df_all.join(xxx, lsuffix='', rsuffix='_20')

        #df_all['chg_rank_20']=df_all.groupby('trade_date')['chg_rank_3'].rank(pct=True)
        #df_all['chg_rank_20']=df_all['chg_rank_20']*10//1

        #print(df_all)

        #10日均量
        xxx=df_all.groupby('ts_code')['amount'].rolling(10).mean().reset_index()
        xxx.set_index(['level_1'], drop=True, append=False, inplace=True, verify_integrity=False)
        xxx.drop(['ts_code'],axis=1,inplace=True)
        df_all=df_all.join(xxx, lsuffix='', rsuffix='_10')

        #当日量占比
        df_all['pst_amount']=df_all['amount']/df_all['amount_10']
        df_all.drop(['amount_10'],axis=1,inplace=True)
        #当日量排名
        df_all['pst_amount_rank_10']=df_all.groupby('trade_date')['pst_amount'].rank(pct=True)
        df_all['pst_amount_rank_10']=df_all['pst_amount_rank_10']*10//2

        ##5日均量
        #xxx=df_all.groupby('ts_code')['amount'].rolling(5).mean().reset_index()
        #xxx.set_index(['level_1'], drop=True, append=False, inplace=True, verify_integrity=False)
        #xxx.drop(['ts_code'],axis=1,inplace=True)
        #df_all=df_all.join(xxx, lsuffix='', rsuffix='_5')

        ##当日量占比
        #df_all['pst_amount']=df_all['amount']/df_all['amount_5']
        #df_all.drop(['amount_5'],axis=1,inplace=True)
        ##当日量排名
        #df_all['pst_amount_rank_5']=df_all.groupby('trade_date')['pst_amount'].rank(pct=True)
        #df_all['pst_amount_rank_5']=df_all['pst_amount_rank_5']*10//1

        #计算三种比例rank
        dolist=['open','high','low']

        for curc in dolist:
            buffer=((df_all[curc]-df_all['pre_close'])*100)/df_all['close']
            df_all[curc]=buffer
            df_all[curc]=df_all.groupby('trade_date')[curc].rank(pct=True)
            df_all[curc]=df_all[curc]*10//2

        #加入昨日rank
        df_all['yesterday_open']=df_all.groupby('ts_code')['open'].shift(1)
        df_all['yesterday_high']=df_all.groupby('ts_code')['high'].shift(1)
        df_all['yesterday_low']=df_all.groupby('ts_code')['low'].shift(1)
        df_all['yesterday_pst_amount_rank']=df_all.groupby('ts_code')['pst_amount_rank_10'].shift(2)
        ##加入前日open
        df_all['yesterday2_open']=df_all.groupby('ts_code')['open'].shift(2)
        df_all['yesterday2_high']=df_all.groupby('ts_code')['high'].shift(2)
        df_all['yesterday2_low']=df_all.groupby('ts_code')['low'].shift(2)
        df_all['yesterday2_pst_amount_rank']=df_all.groupby('ts_code')['pst_amount_rank_10'].shift(2)
        ##加入前日open
        df_all['yesterday3_open']=df_all.groupby('ts_code')['open'].shift(3)
        df_all['yesterday3_high']=df_all.groupby('ts_code')['high'].shift(3)
        df_all['yesterday3_low']=df_all.groupby('ts_code')['low'].shift(3)
        df_all['yesterday3_pst_amount_rank']=df_all.groupby('ts_code')['pst_amount_rank_10'].shift(3)

        df_all.drop(['close','pre_close','pct_chg','pst_amount','adj_factor','real_price','amount'],axis=1,inplace=True)
        #df_all.drop(['close','pre_close','pct_chg','adj_factor','real_price'],axis=1,inplace=True)

        #暂时不用的列
        df_all=df_all[df_all['high_stop']==0]

        #'tomorrow_chg'
        df_all.drop(['high_stop'],axis=1,inplace=True) 

        df_all.dropna(axis=0,how='any',inplace=True)

        print(df_all)
        df_all=df_all.reset_index(drop=True)

        return df_all

    def real_FE(self,gap_day):

        df_data=pd.read_csv('real_now.csv',index_col=0,header=0)
        df_adj_all=pd.read_csv('real_adj_now.csv',index_col=0,header=0)

        df_all=pd.merge(df_data, df_adj_all, how='left', on=['ts_code','trade_date'])

        #加入间隔
        df_all['gap_day']=gap_day

        #df_all=pd.read_csv(bufferstring,index_col=0,header=0,nrows=100000)
    
        #df_all.drop(['change','vol'],axis=1,inplace=True)
 

        #===================================================================================================================================#

        #复权后价格
        df_all['adj_factor']=df_all['adj_factor'].fillna(0)
        df_all['real_price']=df_all['close']*df_all['adj_factor']
        
        df_all['real_price']=df_all.groupby('ts_code')['real_price'].shift(1)
        df_all['real_price']=df_all['real_price']*(1+df_all['pct_chg']/100)


        #30日最低比值
        xxx=df_all.groupby('ts_code')['real_price'].rolling(30).min().reset_index()
        xxx.set_index(['level_1'], drop=True, append=False, inplace=True, verify_integrity=False)
        xxx.drop(['ts_code'],axis=1,inplace=True)
        
        df_all=df_all.join(xxx, lsuffix='', rsuffix='_30min')
        #bbb=df_all.groupby('ts_code')['real_price'].agg({'all_min':np.min, 'all_max': np.max}).reset_index()
        #ccc=pd.merge(df_all, bbb, how='inner', on=['ts_code'])
        df_all['30_pct']=(df_all['real_price']-df_all['real_price_30min'])/df_all['real_price_30min']
        df_all['30_pct_rank']=df_all.groupby('trade_date')['30_pct'].rank(pct=True)
        df_all['30_pct_rank']=df_all['30_pct_rank']*10//1

        #30日最高比值
        xxx=df_all.groupby('ts_code')['real_price'].rolling(30).max().reset_index()
        xxx.set_index(['level_1'], drop=True, append=False, inplace=True, verify_integrity=False)
        xxx.drop(['ts_code'],axis=1,inplace=True)
        
        df_all=df_all.join(xxx, lsuffix='', rsuffix='_30max')
        #bbb=df_all.groupby('ts_code')['real_price'].agg({'all_min':np.min, 'all_max': np.max}).reset_index()
        #ccc=pd.merge(df_all, bbb, how='inner', on=['ts_code'])
        df_all['30_pct_max']=(df_all['real_price']-df_all['real_price_30max'])/df_all['real_price_30max']
        df_all['30_pct_max_rank']=df_all.groupby('trade_date')['30_pct_max'].rank(pct=True)
        df_all['30_pct_max_rank']=df_all['30_pct_max_rank']*10//1

        df_all.drop(['30_pct','real_price_30min','30_pct_max','real_price_30max'],axis=1,inplace=True)


        #===================================================================================================================================#


        #是否停
        df_all['high_stop']=0
        df_all.loc[df_all['pct_chg']>9,'high_stop']=1
        df_all.loc[(df_all['pct_chg']<5.5) & (4.5<df_all['pct_chg']),'high_stop']=1


        #真实价格范围
        df_all['price_real_rank']=df_all.groupby('trade_date')['pre_close'].rank(pct=True)
        df_all['price_real_rank']=df_all['price_real_rank']*10//1
        #1日
        df_all['chg_rank']=df_all.groupby('trade_date')['pct_chg'].rank(pct=True)
        df_all['chg_rank']=df_all['chg_rank']*10//1


        #6日
        xxx=df_all.groupby('ts_code')['chg_rank'].rolling(6).sum().reset_index()
        xxx.set_index(['level_1'], drop=True, append=False, inplace=True, verify_integrity=False)
        xxx.drop(['ts_code'],axis=1,inplace=True)

        df_all=df_all.join(xxx, lsuffix='', rsuffix='_6')

        df_all['chg_rank_6']=df_all.groupby('trade_date')['chg_rank_6'].rank(pct=True)
        df_all['chg_rank_6']=df_all['chg_rank_6']*10//1

        #10日
        xxx=df_all.groupby('ts_code')['chg_rank'].rolling(10).sum().reset_index()
        xxx.set_index(['level_1'], drop=True, append=False, inplace=True, verify_integrity=False)
        xxx.drop(['ts_code'],axis=1,inplace=True)

        df_all=df_all.join(xxx, lsuffix='', rsuffix='_10')

        df_all['chg_rank_10']=df_all.groupby('trade_date')['chg_rank_10'].rank(pct=True)
        df_all['chg_rank_10']=df_all['chg_rank_10']*10//1

        #3日
        xxx=df_all.groupby('ts_code')['chg_rank'].rolling(3).sum().reset_index()
        xxx.set_index(['level_1'], drop=True, append=False, inplace=True, verify_integrity=False)
        xxx.drop(['ts_code'],axis=1,inplace=True)

        df_all=df_all.join(xxx, lsuffix='', rsuffix='_3')

        df_all['chg_rank_3']=df_all.groupby('trade_date')['chg_rank_3'].rank(pct=True)
        df_all['chg_rank_3']=df_all['chg_rank_3']*10//1

        #print(df_all)

        #10日均量
        xxx=df_all.groupby('ts_code')['amount'].rolling(10).mean().reset_index()
        xxx.set_index(['level_1'], drop=True, append=False, inplace=True, verify_integrity=False)
        xxx.drop(['ts_code'],axis=1,inplace=True)
        df_all=df_all.join(xxx, lsuffix='_1', rsuffix='_10')

        #当日量占比
        df_all['pst_amount']=df_all['amount_1']/df_all['amount_10']
        df_all.drop(['amount_1','amount_10'],axis=1,inplace=True)
        #当日量排名
        df_all['pst_amount_rank']=df_all.groupby('trade_date')['pst_amount'].rank(pct=True)
        df_all['pst_amount_rank']=df_all['pst_amount_rank']*10//1

        #计算三种比例rank
        dolist=['open','high','low']

        for curc in dolist:
            buffer=((df_all[curc]-df_all['pre_close'])*100)/df_all['pre_close']
            df_all[curc]=buffer
            df_all[curc]=df_all.groupby('trade_date')[curc].rank(pct=True)
            df_all[curc]=df_all[curc]*10//1

        #加入昨日rank
        df_all['yesterday_open']=df_all.groupby('ts_code')['open'].shift(1)
        df_all['yesterday_high']=df_all.groupby('ts_code')['high'].shift(1)
        df_all['yesterday_low']=df_all.groupby('ts_code')['low'].shift(1)
        df_all['yesterday_pst_amount_rank']=df_all.groupby('ts_code')['pst_amount_rank'].shift(1)
        #加入前日open
        df_all['yesterday2_open']=df_all.groupby('ts_code')['open'].shift(2)

        df_all.drop(['close','pre_close','pct_chg','pst_amount','adj_factor','real_price'],axis=1,inplace=True)
        #暂时不用的列
        df_all=df_all[df_all['high_stop']==0]
        #'tomorrow_chg'
        df_all.drop(['high_stop'],axis=1,inplace=True)



        df_all.dropna(axis=0,how='any',inplace=True)


        month_sec=df_all['trade_date'].max()
        df_all=df_all[df_all['trade_date']!=month_sec]
        print(df_all)
        df_all=df_all.reset_index(drop=True)

        df_all.to_csv('today_train.csv')
        dwdw=1

class FEf(FEbase):
    def __init__(self):
        pass

    def core(self,DataSetName):

        df_data=pd.read_csv(DataSetName[0],index_col=0,header=0)
        df_adj_all=pd.read_csv(DataSetName[1],index_col=0,header=0)
        #df_long_all=pd.read_csv(DataSetName[2],index_col=0,header=0)

        df_all=pd.merge(df_data, df_adj_all, how='inner', on=['ts_code','trade_date'])
        #df_all=pd.merge(df_all, df_long_all, how='inner', on=['ts_code','trade_date'])

        #===================================================================================================================================#
        #df_all['pe'] = df_all['pe'].fillna(9999)
        #df_all['pb'] = df_all['pb'].fillna(9999)

        #df_all['pe_rank']=df_all.groupby('trade_date')['pe'].rank(pct=True)
        #df_all['pb_rank']=df_all.groupby('trade_date')['pb'].rank(pct=True)        
        #df_all['pe_rank']=df_all['pe_rank']*10//1
        #df_all['pb_rank']=df_all['pb_rank']*10//1

        #df_all.drop(['turnover_rate','volume_ratio','pe','pb'],axis=1,inplace=True)

        #print(df_all)
        #df_all.to_csv('sjefosia.csv')

        #===================================================================================================================================#

        ##排除科创版
        #print(df_all)
        df_all=df_all[df_all['ts_code'].str.startswith('688')==False]
        

        ##加入gap_day特征
        #start=df_all['trade_date'].apply(str)[0]
        #end=df_all['trade_date'].apply(str)[df_all.shape[0]-1]
        #xxx=pd.date_range(start,end)

        #df = pd.DataFrame(xxx)
        #df.columns = ['trade_date']
        #df['trade_date']=df['trade_date'].map(str).map(lambda x : x[:4]+x[5:7]+x[8:10]).astype("int64")

        #yyy=df_all['trade_date']
        #zzz2=yyy.unique()
        #df_2=pd.DataFrame(zzz2)
        #df_2.columns = ['trade_date']
        #df_2['day_flag']=1
    
        #result = pd.merge(df, df_2, how='left', on=['trade_date'])
        #result['day_flag2']=result['day_flag'].shift(-1)
        #result['gap_day']=0

        #result.loc[(result['day_flag']==1) & (result['day_flag2']!=1),'gap_day']=1

        #result.drop(['day_flag','day_flag2'],axis=1,inplace=True)

        #df_all=pd.merge(df_all, result, how='left', on=['trade_date'])

        #===================================================================================================================================#

        #复权后价格
        df_all['real_price']=df_all['close']*df_all['adj_factor']
        
        #30日最低比值
        xxx=df_all.groupby('ts_code')['real_price'].rolling(30).min().reset_index()
        xxx.set_index(['level_1'], drop=True, append=False, inplace=True, verify_integrity=False)
        xxx.drop(['ts_code'],axis=1,inplace=True)
        
        df_all=df_all.join(xxx, lsuffix='', rsuffix='_30min')
        #bbb=df_all.groupby('ts_code')['real_price'].agg({'all_min':np.min, 'all_max': np.max}).reset_index()
        #ccc=pd.merge(df_all, bbb, how='inner', on=['ts_code'])
        df_all['30_pct']=(df_all['real_price']-df_all['real_price_30min'])/df_all['real_price_30min']
        df_all['30_pct_rank']=df_all.groupby('trade_date')['30_pct'].rank(pct=True)
        df_all['30_pct_rank']=df_all['30_pct_rank']*10//2

        #30日最高比值
        xxx=df_all.groupby('ts_code')['real_price'].rolling(30).max().reset_index()
        xxx.set_index(['level_1'], drop=True, append=False, inplace=True, verify_integrity=False)
        xxx.drop(['ts_code'],axis=1,inplace=True)
        
        df_all=df_all.join(xxx, lsuffix='', rsuffix='_30max')
        #bbb=df_all.groupby('ts_code')['real_price'].agg({'all_min':np.min, 'all_max': np.max}).reset_index()
        #ccc=pd.merge(df_all, bbb, how='inner', on=['ts_code'])
        df_all['30_pct_max']=(df_all['real_price']-df_all['real_price_30max'])/df_all['real_price_30max']
        df_all['30_pct_max_rank']=df_all.groupby('trade_date')['30_pct_max'].rank(pct=True)
        df_all['30_pct_max_rank']=df_all['30_pct_max_rank']*10//2


        #60日最低比值
        xxx=df_all.groupby('ts_code')['real_price'].rolling(60).min().reset_index()
        xxx.set_index(['level_1'], drop=True, append=False, inplace=True, verify_integrity=False)
        xxx.drop(['ts_code'],axis=1,inplace=True)
        
        df_all=df_all.join(xxx, lsuffix='', rsuffix='_60min')
        #bbb=df_all.groupby('ts_code')['real_price'].agg({'all_min':np.min, 'all_max': np.max}).reset_index()
        #ccc=pd.merge(df_all, bbb, how='inner', on=['ts_code'])
        df_all['60_pct']=(df_all['real_price']-df_all['real_price_60min'])/df_all['real_price_60min']
        df_all['60_pct_rank']=df_all.groupby('trade_date')['60_pct'].rank(pct=True)
        df_all['60_pct_rank']=df_all['60_pct_rank']*10//2

        #60日最高比值
        xxx=df_all.groupby('ts_code')['real_price'].rolling(60).max().reset_index()
        xxx.set_index(['level_1'], drop=True, append=False, inplace=True, verify_integrity=False)
        xxx.drop(['ts_code'],axis=1,inplace=True)
        
        df_all=df_all.join(xxx, lsuffix='', rsuffix='_60max')
        #bbb=df_all.groupby('ts_code')['real_price'].agg({'all_min':np.min, 'all_max': np.max}).reset_index()
        #ccc=pd.merge(df_all, bbb, how='inner', on=['ts_code'])
        df_all['60_pct_max']=(df_all['real_price']-df_all['real_price_60max'])/df_all['real_price_60max']
        df_all['60_pct_max_rank']=df_all.groupby('trade_date')['60_pct_max'].rank(pct=True)
        df_all['60_pct_max_rank']=df_all['60_pct_max_rank']*10//2


        df_all.drop(['30_pct','real_price_30min','30_pct_max','real_price_30max'],axis=1,inplace=True)
        df_all.drop(['60_pct','real_price_60min','60_pct_max','real_price_60max'],axis=1,inplace=True)

        df_all.drop(['change','vol'],axis=1,inplace=True)

        #===================================================================================================================================#
    
        #明日幅度
        tm1=df_all.groupby('ts_code')['pct_chg'].shift(-1)
        tm2=df_all.groupby('ts_code')['pct_chg'].shift(-2)
        #df_all['tomorrow_chg']=((100+tm1)*(100+tm2)-10000)/100
        tm3=df_all.groupby('ts_code')['pct_chg'].shift(-3)
        tm4=df_all.groupby('ts_code')['pct_chg'].shift(-4)
        tm5=df_all.groupby('ts_code')['pct_chg'].shift(-5)

        df_all['tomorrow_chg']=(((100+tm1)/100)*((100+tm2)/100)*((100+tm3)/100)*((100+tm4)/100)*((100+tm5)/100)-1)*100

        #df_all['tomorrow_chg']=((100+tm1)*(100+tm2)*(100+tm3)-1000000)/10000
        #df_all['tomorrow_chg']=df_all.groupby('ts_code')['pct_chg'].shift(-1)
        #明日排名
        df_all['tomorrow_chg_rank']=df_all.groupby('trade_date')['tomorrow_chg'].rank(pct=True)
        df_all['tomorrow_chg_rank']=df_all['tomorrow_chg_rank']*9.9//1
        #是否停
        df_all['high_stop']=0
        df_all.loc[df_all['pct_chg']>9.4,'high_stop']=1
        df_all.loc[(df_all['pct_chg']<5.2) & (4.8<df_all['pct_chg']),'high_stop']=1


        ###真实价格范围(区分实际股价高低)
        #df_all['price_real_rank']=df_all.groupby('trade_date')['pre_close'].rank(pct=True)
        #df_all['price_real_rank']=df_all['price_real_rank']*10//1
        #1日
        df_all['chg_rank']=df_all.groupby('trade_date')['pct_chg'].rank(pct=True)
        df_all['chg_rank']=df_all['chg_rank']*10//2


        #6日
        xxx=df_all.groupby('ts_code')['chg_rank'].rolling(6).sum().reset_index()
        xxx.set_index(['level_1'], drop=True, append=False, inplace=True, verify_integrity=False)
        xxx.drop(['ts_code'],axis=1,inplace=True)

        df_all=df_all.join(xxx, lsuffix='', rsuffix='_6')

        df_all['chg_rank_6']=df_all.groupby('trade_date')['chg_rank_6'].rank(pct=True)
        df_all['chg_rank_6']=df_all['chg_rank_6']*10//2

        #10日
        xxx=df_all.groupby('ts_code')['chg_rank'].rolling(10).sum().reset_index()
        xxx.set_index(['level_1'], drop=True, append=False, inplace=True, verify_integrity=False)
        xxx.drop(['ts_code'],axis=1,inplace=True)

        df_all=df_all.join(xxx, lsuffix='', rsuffix='_10')

        df_all['chg_rank_10']=df_all.groupby('trade_date')['chg_rank_10'].rank(pct=True)
        df_all['chg_rank_10']=df_all['chg_rank_10']*10//2

        #3日
        xxx=df_all.groupby('ts_code')['chg_rank'].rolling(3).sum().reset_index()
        xxx.set_index(['level_1'], drop=True, append=False, inplace=True, verify_integrity=False)
        xxx.drop(['ts_code'],axis=1,inplace=True)

        df_all=df_all.join(xxx, lsuffix='', rsuffix='_3')

        df_all['chg_rank_3']=df_all.groupby('trade_date')['chg_rank_3'].rank(pct=True)
        df_all['chg_rank_3']=df_all['chg_rank_3']*10//2

        ##20日
        #xxx=df_all.groupby('ts_code')['chg_rank'].rolling(20).sum().reset_index()
        #xxx.set_index(['level_1'], drop=True, append=False, inplace=True, verify_integrity=False)
        #xxx.drop(['ts_code'],axis=1,inplace=True)

        #df_all=df_all.join(xxx, lsuffix='', rsuffix='_20')

        #df_all['chg_rank_20']=df_all.groupby('trade_date')['chg_rank_3'].rank(pct=True)
        #df_all['chg_rank_20']=df_all['chg_rank_20']*10//1

        #print(df_all)

        #10日均量
        xxx=df_all.groupby('ts_code')['amount'].rolling(10).mean().reset_index()
        xxx.set_index(['level_1'], drop=True, append=False, inplace=True, verify_integrity=False)
        xxx.drop(['ts_code'],axis=1,inplace=True)
        df_all=df_all.join(xxx, lsuffix='', rsuffix='_10')

        #当日量占比
        df_all['pst_amount']=df_all['amount']/df_all['amount_10']
        df_all.drop(['amount_10'],axis=1,inplace=True)
        #当日量排名
        df_all['pst_amount_rank_10']=df_all.groupby('trade_date')['pst_amount'].rank(pct=True)
        df_all['pst_amount_rank_10']=df_all['pst_amount_rank_10']*10//2

        ##5日均量
        #xxx=df_all.groupby('ts_code')['amount'].rolling(5).mean().reset_index()
        #xxx.set_index(['level_1'], drop=True, append=False, inplace=True, verify_integrity=False)
        #xxx.drop(['ts_code'],axis=1,inplace=True)
        #df_all=df_all.join(xxx, lsuffix='', rsuffix='_5')

        ##当日量占比
        #df_all['pst_amount']=df_all['amount']/df_all['amount_5']
        #df_all.drop(['amount_5'],axis=1,inplace=True)
        ##当日量排名
        #df_all['pst_amount_rank_5']=df_all.groupby('trade_date')['pst_amount'].rank(pct=True)
        #df_all['pst_amount_rank_5']=df_all['pst_amount_rank_5']*10//1

        #计算三种比例rank
        dolist=['open','high','low']

        for curc in dolist:
            buffer=((df_all[curc]-df_all['pre_close'])*100)/df_all['close']
            df_all[curc]=buffer
            df_all[curc]=df_all.groupby('trade_date')[curc].rank(pct=True)
            df_all[curc]=df_all[curc]*10//2

        #加入昨日rank
        df_all['yesterday_open']=df_all.groupby('ts_code')['open'].shift(1)
        df_all['yesterday_high']=df_all.groupby('ts_code')['high'].shift(1)
        df_all['yesterday_low']=df_all.groupby('ts_code')['low'].shift(1)
        df_all['yesterday_pst_amount_rank']=df_all.groupby('ts_code')['pst_amount_rank_10'].shift(1)
        ##加入前日open
        df_all['yesterday2_open']=df_all.groupby('ts_code')['open'].shift(2)
        df_all['yesterday2_high']=df_all.groupby('ts_code')['high'].shift(2)
        df_all['yesterday2_low']=df_all.groupby('ts_code')['low'].shift(2)
        df_all['yesterday2_pst_amount_rank']=df_all.groupby('ts_code')['pst_amount_rank_10'].shift(2)
        ##加入前日open
        df_all['yesterday3_open']=df_all.groupby('ts_code')['open'].shift(3)
        df_all['yesterday3_high']=df_all.groupby('ts_code')['high'].shift(3)
        df_all['yesterday3_low']=df_all.groupby('ts_code')['low'].shift(3)
        df_all['yesterday3_pst_amount_rank']=df_all.groupby('ts_code')['pst_amount_rank_10'].shift(3)


        df_all.drop(['close','pre_close','pct_chg','pst_amount','adj_factor','real_price','amount'],axis=1,inplace=True)
        #df_all.drop(['close','pre_close','pct_chg','adj_factor','real_price'],axis=1,inplace=True)

        #暂时不用的列
        df_all=df_all[df_all['high_stop']==0]

        #'tomorrow_chg'
        df_all.drop(['high_stop'],axis=1,inplace=True) 

        df_all.dropna(axis=0,how='any',inplace=True)

        print(df_all)
        df_all=df_all.reset_index(drop=True)

        return df_all

    def real_FE(self):

        df_data=pd.read_csv('real_now.csv',index_col=0,header=0)
        df_adj_all=pd.read_csv('real_adj_now.csv',index_col=0,header=0)

        df_all=pd.merge(df_data, df_adj_all, how='left', on=['ts_code','trade_date'])

        df_all=df_all[df_all['ts_code'].str.startswith('688')==False]

        #df_all=pd.read_csv(bufferstring,index_col=0,header=0,nrows=100000)
    
        #df_all.drop(['change','vol'],axis=1,inplace=True)
 

        #===================================================================================================================================#

        #复权后价格
        df_all['adj_factor']=df_all['adj_factor'].fillna(0)
        df_all['real_price']=df_all['close']*df_all['adj_factor']
        
        df_all['real_price']=df_all.groupby('ts_code')['real_price'].shift(1)
        df_all['real_price']=df_all['real_price']*(1+df_all['pct_chg']/100)


        #30日最低比值
        xxx=df_all.groupby('ts_code')['real_price'].rolling(30).min().reset_index()
        xxx.set_index(['level_1'], drop=True, append=False, inplace=True, verify_integrity=False)
        xxx.drop(['ts_code'],axis=1,inplace=True)
        
        df_all=df_all.join(xxx, lsuffix='', rsuffix='_30min')
        #bbb=df_all.groupby('ts_code')['real_price'].agg({'all_min':np.min, 'all_max': np.max}).reset_index()
        #ccc=pd.merge(df_all, bbb, how='inner', on=['ts_code'])
        df_all['30_pct']=(df_all['real_price']-df_all['real_price_30min'])/df_all['real_price_30min']
        df_all['30_pct_rank']=df_all.groupby('trade_date')['30_pct'].rank(pct=True)
        df_all['30_pct_rank']=df_all['30_pct_rank']*10//2

        #30日最高比值
        xxx=df_all.groupby('ts_code')['real_price'].rolling(30).max().reset_index()
        xxx.set_index(['level_1'], drop=True, append=False, inplace=True, verify_integrity=False)
        xxx.drop(['ts_code'],axis=1,inplace=True)
        
        df_all=df_all.join(xxx, lsuffix='', rsuffix='_30max')
        #bbb=df_all.groupby('ts_code')['real_price'].agg({'all_min':np.min, 'all_max': np.max}).reset_index()
        #ccc=pd.merge(df_all, bbb, how='inner', on=['ts_code'])
        df_all['30_pct_max']=(df_all['real_price']-df_all['real_price_30max'])/df_all['real_price_30max']
        df_all['30_pct_max_rank']=df_all.groupby('trade_date')['30_pct_max'].rank(pct=True)
        df_all['30_pct_max_rank']=df_all['30_pct_max_rank']*10//2

        df_all.drop(['30_pct','real_price_30min','30_pct_max','real_price_30max'],axis=1,inplace=True)


        #===================================================================================================================================#


        #是否停
        df_all['high_stop']=0
        df_all.loc[df_all['pct_chg']>9.7,'high_stop']=1
        df_all.loc[(df_all['pct_chg']<5.2) & (4.8<df_all['pct_chg']),'high_stop']=1


        #1日
        df_all['chg_rank']=df_all.groupby('trade_date')['pct_chg'].rank(pct=True)
        df_all['chg_rank']=df_all['chg_rank']*10//2


        #6日
        xxx=df_all.groupby('ts_code')['chg_rank'].rolling(6).sum().reset_index()
        xxx.set_index(['level_1'], drop=True, append=False, inplace=True, verify_integrity=False)
        xxx.drop(['ts_code'],axis=1,inplace=True)

        df_all=df_all.join(xxx, lsuffix='', rsuffix='_6')

        df_all['chg_rank_6']=df_all.groupby('trade_date')['chg_rank_6'].rank(pct=True)
        df_all['chg_rank_6']=df_all['chg_rank_6']*10//2

        #10日
        xxx=df_all.groupby('ts_code')['chg_rank'].rolling(10).sum().reset_index()
        xxx.set_index(['level_1'], drop=True, append=False, inplace=True, verify_integrity=False)
        xxx.drop(['ts_code'],axis=1,inplace=True)

        df_all=df_all.join(xxx, lsuffix='', rsuffix='_10')

        df_all['chg_rank_10']=df_all.groupby('trade_date')['chg_rank_10'].rank(pct=True)
        df_all['chg_rank_10']=df_all['chg_rank_10']*10//2

        #3日
        xxx=df_all.groupby('ts_code')['chg_rank'].rolling(3).sum().reset_index()
        xxx.set_index(['level_1'], drop=True, append=False, inplace=True, verify_integrity=False)
        xxx.drop(['ts_code'],axis=1,inplace=True)

        df_all=df_all.join(xxx, lsuffix='', rsuffix='_3')

        df_all['chg_rank_3']=df_all.groupby('trade_date')['chg_rank_3'].rank(pct=True)
        df_all['chg_rank_3']=df_all['chg_rank_3']*10//2

        #print(df_all)

        #10日均量
        xxx=df_all.groupby('ts_code')['amount'].rolling(10).mean().reset_index()
        xxx.set_index(['level_1'], drop=True, append=False, inplace=True, verify_integrity=False)
        xxx.drop(['ts_code'],axis=1,inplace=True)
        df_all=df_all.join(xxx, lsuffix='_1', rsuffix='_10')

        #当日量占比
        df_all['pst_amount']=df_all['amount_1']/df_all['amount_10']
        df_all.drop(['amount_1','amount_10'],axis=1,inplace=True)
        #当日量排名
        df_all['pst_amount_rank']=df_all.groupby('trade_date')['pst_amount'].rank(pct=True)
        df_all['pst_amount_rank']=df_all['pst_amount_rank']*10//2

        #计算三种比例rank
        dolist=['open','high','low']

        for curc in dolist:
            buffer=((df_all[curc]-df_all['pre_close'])*100)/df_all['pre_close']
            df_all[curc]=buffer
            df_all[curc]=df_all.groupby('trade_date')[curc].rank(pct=True)
            df_all[curc]=df_all[curc]*10//2

        #加入昨日rank
        df_all['yesterday_open']=df_all.groupby('ts_code')['open'].shift(1)
        df_all['yesterday_high']=df_all.groupby('ts_code')['high'].shift(1)
        df_all['yesterday_low']=df_all.groupby('ts_code')['low'].shift(1)
        df_all['yesterday_pst_amount_rank']=df_all.groupby('ts_code')['pst_amount_rank_10'].shift(1)
        ##加入前日open
        df_all['yesterday2_open']=df_all.groupby('ts_code')['open'].shift(2)
        df_all['yesterday2_high']=df_all.groupby('ts_code')['high'].shift(2)
        df_all['yesterday2_low']=df_all.groupby('ts_code')['low'].shift(2)
        df_all['yesterday2_pst_amount_rank']=df_all.groupby('ts_code')['pst_amount_rank_10'].shift(2)
        ##加入前日open
        df_all['yesterday3_open']=df_all.groupby('ts_code')['open'].shift(3)
        df_all['yesterday3_high']=df_all.groupby('ts_code')['high'].shift(3)
        df_all['yesterday3_low']=df_all.groupby('ts_code')['low'].shift(3)
        df_all['yesterday3_pst_amount_rank']=df_all.groupby('ts_code')['pst_amount_rank_10'].shift(3)

        df_all.drop(['close','pre_close','pct_chg','pst_amount','adj_factor','real_price'],axis=1,inplace=True)



        #暂时不用的列
        df_all=df_all[df_all['high_stop']==0]
        #'tomorrow_chg'
        df_all.drop(['high_stop'],axis=1,inplace=True)



        df_all.dropna(axis=0,how='any',inplace=True)


        month_sec=df_all['trade_date'].max()
        df_all=df_all[df_all['trade_date']!=month_sec]
        print(df_all)
        df_all=df_all.reset_index(drop=True)

        df_all.to_csv('today_train.csv')
        dwdw=1

class FEg360(FEbase):
    def __init__(self):
        pass

    def core(self,DataSetName):

        df_data=pd.read_csv(DataSetName[0],index_col=0,header=0)
        df_adj_all=pd.read_csv(DataSetName[1],index_col=0,header=0)
        #df_long_all=pd.read_csv(DataSetName[2],index_col=0,header=0)

        df_all=pd.merge(df_data, df_adj_all, how='inner', on=['ts_code','trade_date'])
        #df_all=pd.merge(df_all, df_long_all, how='inner', on=['ts_code','trade_date'])

        #===================================================================================================================================#
        #df_all['pe'] = df_all['pe'].fillna(9999)
        #df_all['pb'] = df_all['pb'].fillna(9999)

        #df_all['pe_rank']=df_all.groupby('trade_date')['pe'].rank(pct=True)
        #df_all['pb_rank']=df_all.groupby('trade_date')['pb'].rank(pct=True)        
        #df_all['pe_rank']=df_all['pe_rank']*10//1
        #df_all['pb_rank']=df_all['pb_rank']*10//1

        #df_all.drop(['turnover_rate','volume_ratio','pe','pb'],axis=1,inplace=True)

        #print(df_all)
        #df_all.to_csv('sjefosia.csv')

        #===================================================================================================================================#

        ##排除科创版
        #print(df_all)
        df_all=df_all[df_all['ts_code'].str.startswith('688')==False]
        

        ##加入gap_day特征
        #start=df_all['trade_date'].apply(str)[0]
        #end=df_all['trade_date'].apply(str)[df_all.shape[0]-1]
        #xxx=pd.date_range(start,end)

        #df = pd.DataFrame(xxx)
        #df.columns = ['trade_date']
        #df['trade_date']=df['trade_date'].map(str).map(lambda x : x[:4]+x[5:7]+x[8:10]).astype("int64")

        #yyy=df_all['trade_date']
        #zzz2=yyy.unique()
        #df_2=pd.DataFrame(zzz2)
        #df_2.columns = ['trade_date']
        #df_2['day_flag']=1
    
        #result = pd.merge(df, df_2, how='left', on=['trade_date'])
        #result['day_flag2']=result['day_flag'].shift(-1)
        #result['gap_day']=0

        #result.loc[(result['day_flag']==1) & (result['day_flag2']!=1),'gap_day']=1

        #result.drop(['day_flag','day_flag2'],axis=1,inplace=True)

        #df_all=pd.merge(df_all, result, how='left', on=['trade_date'])

        #===================================================================================================================================#

        #复权后价格
        df_all['real_price']=df_all['close']*df_all['adj_factor']
        
        #30日最低比值
        xxx=df_all.groupby('ts_code')['real_price'].rolling(30).min().reset_index()
        xxx.set_index(['level_1'], drop=True, append=False, inplace=True, verify_integrity=False)
        xxx.drop(['ts_code'],axis=1,inplace=True)
        
        df_all=df_all.join(xxx, lsuffix='', rsuffix='_30min')
        #bbb=df_all.groupby('ts_code')['real_price'].agg({'all_min':np.min, 'all_max': np.max}).reset_index()
        #ccc=pd.merge(df_all, bbb, how='inner', on=['ts_code'])
        df_all['30_pct']=(df_all['real_price']-df_all['real_price_30min'])/df_all['real_price_30min']
        df_all['30_pct_rank']=df_all.groupby('trade_date')['30_pct'].rank(pct=True)
        df_all['30_pct_rank']=df_all['30_pct_rank']*10//2

        #30日最高比值
        xxx=df_all.groupby('ts_code')['real_price'].rolling(30).max().reset_index()
        xxx.set_index(['level_1'], drop=True, append=False, inplace=True, verify_integrity=False)
        xxx.drop(['ts_code'],axis=1,inplace=True)
        
        df_all=df_all.join(xxx, lsuffix='', rsuffix='_30max')
        #bbb=df_all.groupby('ts_code')['real_price'].agg({'all_min':np.min, 'all_max': np.max}).reset_index()
        #ccc=pd.merge(df_all, bbb, how='inner', on=['ts_code'])
        df_all['30_pct_max']=(df_all['real_price']-df_all['real_price_30max'])/df_all['real_price_30max']
        df_all['30_pct_max_rank']=df_all.groupby('trade_date')['30_pct_max'].rank(pct=True)
        df_all['30_pct_max_rank']=df_all['30_pct_max_rank']*10//2


        #60日最低比值
        xxx=df_all.groupby('ts_code')['real_price'].rolling(60).min().reset_index()
        xxx.set_index(['level_1'], drop=True, append=False, inplace=True, verify_integrity=False)
        xxx.drop(['ts_code'],axis=1,inplace=True)
        
        df_all=df_all.join(xxx, lsuffix='', rsuffix='_60min')
        #bbb=df_all.groupby('ts_code')['real_price'].agg({'all_min':np.min, 'all_max': np.max}).reset_index()
        #ccc=pd.merge(df_all, bbb, how='inner', on=['ts_code'])
        df_all['60_pct']=(df_all['real_price']-df_all['real_price_60min'])/df_all['real_price_60min']
        df_all['60_pct_rank']=df_all.groupby('trade_date')['60_pct'].rank(pct=True)
        df_all['60_pct_rank']=df_all['60_pct_rank']*10//2

        #60日最高比值
        xxx=df_all.groupby('ts_code')['real_price'].rolling(60).max().reset_index()
        xxx.set_index(['level_1'], drop=True, append=False, inplace=True, verify_integrity=False)
        xxx.drop(['ts_code'],axis=1,inplace=True)
        
        df_all=df_all.join(xxx, lsuffix='', rsuffix='_60max')
        #bbb=df_all.groupby('ts_code')['real_price'].agg({'all_min':np.min, 'all_max': np.max}).reset_index()
        #ccc=pd.merge(df_all, bbb, how='inner', on=['ts_code'])
        df_all['60_pct_max']=(df_all['real_price']-df_all['real_price_60max'])/df_all['real_price_60max']
        df_all['60_pct_max_rank']=df_all.groupby('trade_date')['60_pct_max'].rank(pct=True)
        df_all['60_pct_max_rank']=df_all['60_pct_max_rank']*10//2


        df_all.drop(['30_pct','real_price_30min','30_pct_max','real_price_30max'],axis=1,inplace=True)
        df_all.drop(['60_pct','real_price_60min','60_pct_max','real_price_60max'],axis=1,inplace=True)

        df_all.drop(['change','vol'],axis=1,inplace=True)

        #===================================================================================================================================#
    
        #明日幅度
        tm1=df_all.groupby('ts_code')['pct_chg'].shift(-1)
        tm2=df_all.groupby('ts_code')['pct_chg'].shift(-2)
        #df_all['tomorrow_chg']=((100+tm1)*(100+tm2)-10000)/100
        tm3=df_all.groupby('ts_code')['pct_chg'].shift(-3)
        tm4=df_all.groupby('ts_code')['pct_chg'].shift(-4)
        tm5=df_all.groupby('ts_code')['pct_chg'].shift(-5)

        df_all['tomorrow_chg']=(((100+tm1)/100)*((100+tm2)/100)*((100+tm3)/100)*((100+tm4)/100)*((100+tm5)/100)-1)*100

        #df_all['tomorrow_chg']=((100+tm1)*(100+tm2)*(100+tm3)-1000000)/10000
        #df_all['tomorrow_chg']=df_all.groupby('ts_code')['pct_chg'].shift(-1)
        #明日排名
        df_all['tomorrow_chg_rank']=df_all.groupby('trade_date')['tomorrow_chg'].rank(pct=True)
        df_all['tomorrow_chg_rank']=df_all['tomorrow_chg_rank']*9.9//1
        #是否停
        df_all['high_stop']=0
        df_all.loc[df_all['pct_chg']>9.4,'high_stop']=1
        df_all.loc[(df_all['pct_chg']<5.2) & (4.8<df_all['pct_chg']),'high_stop']=1


        ###真实价格范围(区分实际股价高低)
        #df_all['price_real_rank']=df_all.groupby('trade_date')['pre_close'].rank(pct=True)
        #df_all['price_real_rank']=df_all['price_real_rank']*10//1
        #1日
        df_all['chg_rank']=df_all.groupby('trade_date')['pct_chg'].rank(pct=True)
        df_all['chg_rank']=df_all['chg_rank']*10//2


        #6日
        xxx=df_all.groupby('ts_code')['chg_rank'].rolling(6).sum().reset_index()
        xxx.set_index(['level_1'], drop=True, append=False, inplace=True, verify_integrity=False)
        xxx.drop(['ts_code'],axis=1,inplace=True)

        df_all=df_all.join(xxx, lsuffix='', rsuffix='_6')

        df_all['chg_rank_6']=df_all.groupby('trade_date')['chg_rank_6'].rank(pct=True)
        df_all['chg_rank_6']=df_all['chg_rank_6']*10//2

        #10日
        xxx=df_all.groupby('ts_code')['chg_rank'].rolling(10).sum().reset_index()
        xxx.set_index(['level_1'], drop=True, append=False, inplace=True, verify_integrity=False)
        xxx.drop(['ts_code'],axis=1,inplace=True)

        df_all=df_all.join(xxx, lsuffix='', rsuffix='_10')

        df_all['chg_rank_10']=df_all.groupby('trade_date')['chg_rank_10'].rank(pct=True)
        df_all['chg_rank_10']=df_all['chg_rank_10']*10//2

        #3日
        xxx=df_all.groupby('ts_code')['chg_rank'].rolling(3).sum().reset_index()
        xxx.set_index(['level_1'], drop=True, append=False, inplace=True, verify_integrity=False)
        xxx.drop(['ts_code'],axis=1,inplace=True)

        df_all=df_all.join(xxx, lsuffix='', rsuffix='_3')

        df_all['chg_rank_3']=df_all.groupby('trade_date')['chg_rank_3'].rank(pct=True)
        df_all['chg_rank_3']=df_all['chg_rank_3']*10//2

        ##20日
        #xxx=df_all.groupby('ts_code')['chg_rank'].rolling(20).sum().reset_index()
        #xxx.set_index(['level_1'], drop=True, append=False, inplace=True, verify_integrity=False)
        #xxx.drop(['ts_code'],axis=1,inplace=True)

        #df_all=df_all.join(xxx, lsuffix='', rsuffix='_20')

        #df_all['chg_rank_20']=df_all.groupby('trade_date')['chg_rank_3'].rank(pct=True)
        #df_all['chg_rank_20']=df_all['chg_rank_20']*10//1

        #print(df_all)

        #10日均量
        xxx=df_all.groupby('ts_code')['amount'].rolling(10).mean().reset_index()
        xxx.set_index(['level_1'], drop=True, append=False, inplace=True, verify_integrity=False)
        xxx.drop(['ts_code'],axis=1,inplace=True)
        df_all=df_all.join(xxx, lsuffix='', rsuffix='_10')

        #当日量占比
        df_all['pst_amount']=df_all['amount']/df_all['amount_10']
        df_all.drop(['amount_10'],axis=1,inplace=True)
        #当日量排名
        df_all['pst_amount_rank_10']=df_all.groupby('trade_date')['pst_amount'].rank(pct=True)
        df_all['pst_amount_rank_10']=df_all['pst_amount_rank_10']*10//2

        ##5日均量
        #xxx=df_all.groupby('ts_code')['amount'].rolling(5).mean().reset_index()
        #xxx.set_index(['level_1'], drop=True, append=False, inplace=True, verify_integrity=False)
        #xxx.drop(['ts_code'],axis=1,inplace=True)
        #df_all=df_all.join(xxx, lsuffix='', rsuffix='_5')

        ##当日量占比
        #df_all['pst_amount']=df_all['amount']/df_all['amount_5']
        #df_all.drop(['amount_5'],axis=1,inplace=True)
        ##当日量排名
        #df_all['pst_amount_rank_5']=df_all.groupby('trade_date')['pst_amount'].rank(pct=True)
        #df_all['pst_amount_rank_5']=df_all['pst_amount_rank_5']*10//1

        #计算三种比例rank
        dolist=['open','high','low']

        for curc in dolist:
            buffer=((df_all[curc]-df_all['pre_close'])*100)/df_all['close']
            df_all[curc]=buffer
            df_all[curc]=df_all.groupby('trade_date')[curc].rank(pct=True)
            df_all[curc]=df_all[curc]*10//2

        #加入昨日rank
        df_all['yesterday_chg_rank']=df_all.groupby('ts_code')['chg_rank'].shift(1)
        df_all['yesterday_open']=df_all.groupby('ts_code')['open'].shift(1)
        df_all['yesterday_high']=df_all.groupby('ts_code')['high'].shift(1)
        df_all['yesterday_low']=df_all.groupby('ts_code')['low'].shift(1)
        df_all['yesterday_pst_amount_rank']=df_all.groupby('ts_code')['pst_amount_rank_10'].shift(1)
        ##加入前日open
        df_all['yesterday2_chg_rank']=df_all.groupby('ts_code')['chg_rank'].shift(2)
        df_all['yesterday2_open']=df_all.groupby('ts_code')['open'].shift(2)
        df_all['yesterday2_high']=df_all.groupby('ts_code')['high'].shift(2)
        df_all['yesterday2_low']=df_all.groupby('ts_code')['low'].shift(2)
        df_all['yesterday2_pst_amount_rank']=df_all.groupby('ts_code')['pst_amount_rank_10'].shift(2)
        ##加入前日open
        df_all['yesterday3_chg_rank']=df_all.groupby('ts_code')['chg_rank'].shift(3)
        df_all['yesterday3_open']=df_all.groupby('ts_code')['open'].shift(3)
        df_all['yesterday3_high']=df_all.groupby('ts_code')['high'].shift(3)
        df_all['yesterday3_low']=df_all.groupby('ts_code')['low'].shift(3)
        df_all['yesterday3_pst_amount_rank']=df_all.groupby('ts_code')['pst_amount_rank_10'].shift(3)


        df_all.drop(['close','pre_close','pct_chg','pst_amount','adj_factor','real_price','amount'],axis=1,inplace=True)
        #df_all.drop(['close','pre_close','pct_chg','adj_factor','real_price'],axis=1,inplace=True)

        #暂时不用的列
        df_all=df_all[df_all['high_stop']==0]

        #'tomorrow_chg'
        df_all.drop(['high_stop'],axis=1,inplace=True) 

        df_all.dropna(axis=0,how='any',inplace=True)

        print(df_all)
        df_all=df_all.reset_index(drop=True)

        return df_all

    def real_FE(self):

        df_data=pd.read_csv('real_now.csv',index_col=0,header=0)
        df_adj_all=pd.read_csv('real_adj_now.csv',index_col=0,header=0)

        df_all=pd.merge(df_data, df_adj_all, how='left', on=['ts_code','trade_date'])

        #df_all=df_all[df_all['ts_code'].str.startswith('688')==False]

        #df_all=pd.read_csv(bufferstring,index_col=0,header=0,nrows=100000)
    
        #df_all.drop(['change','vol'],axis=1,inplace=True)
 

        #===================================================================================================================================#

        #复权后价格
        df_all['adj_factor']=df_all['adj_factor'].fillna(0)
        df_all['real_price']=df_all['close']*df_all['adj_factor']
        
        df_all['real_price']=df_all.groupby('ts_code')['real_price'].shift(1)
        df_all['real_price']=df_all['real_price']*(1+df_all['pct_chg']/100)


        #30日最低比值
        xxx=df_all.groupby('ts_code')['real_price'].rolling(30).min().reset_index()
        xxx.set_index(['level_1'], drop=True, append=False, inplace=True, verify_integrity=False)
        xxx.drop(['ts_code'],axis=1,inplace=True)
        
        df_all=df_all.join(xxx, lsuffix='', rsuffix='_30min')
        #bbb=df_all.groupby('ts_code')['real_price'].agg({'all_min':np.min, 'all_max': np.max}).reset_index()
        #ccc=pd.merge(df_all, bbb, how='inner', on=['ts_code'])
        df_all['30_pct']=(df_all['real_price']-df_all['real_price_30min'])/df_all['real_price_30min']
        df_all['30_pct_rank']=df_all.groupby('trade_date')['30_pct'].rank(pct=True)
        df_all['30_pct_rank']=df_all['30_pct_rank']*10//2

        #30日最高比值
        xxx=df_all.groupby('ts_code')['real_price'].rolling(30).max().reset_index()
        xxx.set_index(['level_1'], drop=True, append=False, inplace=True, verify_integrity=False)
        xxx.drop(['ts_code'],axis=1,inplace=True)
        
        df_all=df_all.join(xxx, lsuffix='', rsuffix='_30max')
        #bbb=df_all.groupby('ts_code')['real_price'].agg({'all_min':np.min, 'all_max': np.max}).reset_index()
        #ccc=pd.merge(df_all, bbb, how='inner', on=['ts_code'])
        df_all['30_pct_max']=(df_all['real_price']-df_all['real_price_30max'])/df_all['real_price_30max']
        df_all['30_pct_max_rank']=df_all.groupby('trade_date')['30_pct_max'].rank(pct=True)
        df_all['30_pct_max_rank']=df_all['30_pct_max_rank']*10//2

        df_all.drop(['30_pct','real_price_30min','30_pct_max','real_price_30max'],axis=1,inplace=True)


        #===================================================================================================================================#


        #是否停
        df_all['high_stop']=0
        df_all.loc[df_all['pct_chg']>9.7,'high_stop']=1
        df_all.loc[(df_all['pct_chg']<5.2) & (4.8<df_all['pct_chg']),'high_stop']=1


        #1日
        df_all['chg_rank']=df_all.groupby('trade_date')['pct_chg'].rank(pct=True)
        df_all['chg_rank']=df_all['chg_rank']*10//2


        #6日
        xxx=df_all.groupby('ts_code')['chg_rank'].rolling(6).sum().reset_index()
        xxx.set_index(['level_1'], drop=True, append=False, inplace=True, verify_integrity=False)
        xxx.drop(['ts_code'],axis=1,inplace=True)

        df_all=df_all.join(xxx, lsuffix='', rsuffix='_6')

        df_all['chg_rank_6']=df_all.groupby('trade_date')['chg_rank_6'].rank(pct=True)
        df_all['chg_rank_6']=df_all['chg_rank_6']*10//2

        #10日
        xxx=df_all.groupby('ts_code')['chg_rank'].rolling(10).sum().reset_index()
        xxx.set_index(['level_1'], drop=True, append=False, inplace=True, verify_integrity=False)
        xxx.drop(['ts_code'],axis=1,inplace=True)

        df_all=df_all.join(xxx, lsuffix='', rsuffix='_10')

        df_all['chg_rank_10']=df_all.groupby('trade_date')['chg_rank_10'].rank(pct=True)
        df_all['chg_rank_10']=df_all['chg_rank_10']*10//2

        #3日
        xxx=df_all.groupby('ts_code')['chg_rank'].rolling(3).sum().reset_index()
        xxx.set_index(['level_1'], drop=True, append=False, inplace=True, verify_integrity=False)
        xxx.drop(['ts_code'],axis=1,inplace=True)

        df_all=df_all.join(xxx, lsuffix='', rsuffix='_3')

        df_all['chg_rank_3']=df_all.groupby('trade_date')['chg_rank_3'].rank(pct=True)
        df_all['chg_rank_3']=df_all['chg_rank_3']*10//2

        #print(df_all)

        #10日均量
        xxx=df_all.groupby('ts_code')['amount'].rolling(10).mean().reset_index()
        xxx.set_index(['level_1'], drop=True, append=False, inplace=True, verify_integrity=False)
        xxx.drop(['ts_code'],axis=1,inplace=True)
        df_all=df_all.join(xxx, lsuffix='', rsuffix='_10')

        #当日量占比
        df_all['pst_amount']=df_all['amount']/df_all['amount_10']
        df_all.drop(['amount_10'],axis=1,inplace=True)
        #当日量排名
        df_all['pst_amount_rank_10']=df_all.groupby('trade_date')['pst_amount'].rank(pct=True)
        df_all['pst_amount_rank_10']=df_all['pst_amount_rank_10']*10//2

        #计算三种比例rank
        dolist=['open','high','low']

        for curc in dolist:
            buffer=((df_all[curc]-df_all['pre_close'])*100)/df_all['pre_close']
            df_all[curc]=buffer
            df_all[curc]=df_all.groupby('trade_date')[curc].rank(pct=True)
            df_all[curc]=df_all[curc]*10//2

        #加入昨日rank
        df_all['yesterday_open']=df_all.groupby('ts_code')['open'].shift(1)
        df_all['yesterday_high']=df_all.groupby('ts_code')['high'].shift(1)
        df_all['yesterday_low']=df_all.groupby('ts_code')['low'].shift(1)
        df_all['yesterday_pst_amount_rank']=df_all.groupby('ts_code')['pst_amount_rank_10'].shift(1)
        ##加入前日open
        df_all['yesterday2_open']=df_all.groupby('ts_code')['open'].shift(2)
        df_all['yesterday2_high']=df_all.groupby('ts_code')['high'].shift(2)
        df_all['yesterday2_low']=df_all.groupby('ts_code')['low'].shift(2)
        df_all['yesterday2_pst_amount_rank']=df_all.groupby('ts_code')['pst_amount_rank_10'].shift(2)
        ##加入前日open
        df_all['yesterday3_open']=df_all.groupby('ts_code')['open'].shift(3)
        df_all['yesterday3_high']=df_all.groupby('ts_code')['high'].shift(3)
        df_all['yesterday3_low']=df_all.groupby('ts_code')['low'].shift(3)
        df_all['yesterday3_pst_amount_rank']=df_all.groupby('ts_code')['pst_amount_rank_10'].shift(3)

        df_all.drop(['close','pre_close','pct_chg','pst_amount','adj_factor','real_price','amount'],axis=1,inplace=True)



        #暂时不用的列
        df_all=df_all[df_all['high_stop']==0]
        #'tomorrow_chg'
        df_all.drop(['high_stop'],axis=1,inplace=True)



        df_all.dropna(axis=0,how='any',inplace=True)


        month_sec=df_all['trade_date'].max()
        df_all=df_all[df_all['trade_date']!=month_sec]
        print(df_all)
        df_all=df_all.reset_index(drop=True)

        df_all.to_csv('today_train.csv')
        dwdw=1

class FEg20(FEbase):
    def __init__(self):
        pass

    def core(self,DataSetName):

        df_data=pd.read_csv(DataSetName[0],index_col=0,header=0)
        df_adj_all=pd.read_csv(DataSetName[1],index_col=0,header=0)
        #df_long_all=pd.read_csv(DataSetName[2],index_col=0,header=0)

        df_all=pd.merge(df_data, df_adj_all, how='inner', on=['ts_code','trade_date'])
        #df_all=pd.merge(df_all, df_long_all, how='inner', on=['ts_code','trade_date'])

        #===================================================================================================================================#
        #df_all['pe'] = df_all['pe'].fillna(9999)
        #df_all['pb'] = df_all['pb'].fillna(9999)

        #df_all['pe_rank']=df_all.groupby('trade_date')['pe'].rank(pct=True)
        #df_all['pb_rank']=df_all.groupby('trade_date')['pb'].rank(pct=True)        
        #df_all['pe_rank']=df_all['pe_rank']*10//1
        #df_all['pb_rank']=df_all['pb_rank']*10//1

        #df_all.drop(['turnover_rate','volume_ratio','pe','pb'],axis=1,inplace=True)

        #print(df_all)
        #df_all.to_csv('sjefosia.csv')

        #===================================================================================================================================#

        ##排除科创版
        #print(df_all)
        df_all=df_all[df_all['ts_code'].str.startswith('688')==False]
        

        ##加入gap_day特征
        #start=df_all['trade_date'].apply(str)[0]
        #end=df_all['trade_date'].apply(str)[df_all.shape[0]-1]
        #xxx=pd.date_range(start,end)

        #df = pd.DataFrame(xxx)
        #df.columns = ['trade_date']
        #df['trade_date']=df['trade_date'].map(str).map(lambda x : x[:4]+x[5:7]+x[8:10]).astype("int64")

        #yyy=df_all['trade_date']
        #zzz2=yyy.unique()
        #df_2=pd.DataFrame(zzz2)
        #df_2.columns = ['trade_date']
        #df_2['day_flag']=1
    
        #result = pd.merge(df, df_2, how='left', on=['trade_date'])
        #result['day_flag2']=result['day_flag'].shift(-1)
        #result['gap_day']=0

        #result.loc[(result['day_flag']==1) & (result['day_flag2']!=1),'gap_day']=1

        #result.drop(['day_flag','day_flag2'],axis=1,inplace=True)

        #df_all=pd.merge(df_all, result, how='left', on=['trade_date'])

        #===================================================================================================================================#

        #复权后价格
        df_all['real_price']=df_all['close']*df_all['adj_factor']
        
        ##30日最低比值
        #xxx=df_all.groupby('ts_code')['real_price'].rolling(30).min().reset_index()
        #xxx.set_index(['level_1'], drop=True, append=False, inplace=True, verify_integrity=False)
        #xxx.drop(['ts_code'],axis=1,inplace=True)
        
        #df_all=df_all.join(xxx, lsuffix='', rsuffix='_30min')
        ##bbb=df_all.groupby('ts_code')['real_price'].agg({'all_min':np.min, 'all_max': np.max}).reset_index()
        ##ccc=pd.merge(df_all, bbb, how='inner', on=['ts_code'])
        #df_all['30_pct']=(df_all['real_price']-df_all['real_price_30min'])/df_all['real_price_30min']
        #df_all['30_pct_rank']=df_all.groupby('trade_date')['30_pct'].rank(pct=True)
        #df_all['30_pct_rank']=df_all['30_pct_rank']*10//2

        ##30日最高比值
        #xxx=df_all.groupby('ts_code')['real_price'].rolling(30).max().reset_index()
        #xxx.set_index(['level_1'], drop=True, append=False, inplace=True, verify_integrity=False)
        #xxx.drop(['ts_code'],axis=1,inplace=True)
        
        #df_all=df_all.join(xxx, lsuffix='', rsuffix='_30max')
        ##bbb=df_all.groupby('ts_code')['real_price'].agg({'all_min':np.min, 'all_max': np.max}).reset_index()
        ##ccc=pd.merge(df_all, bbb, how='inner', on=['ts_code'])
        #df_all['30_pct_max']=(df_all['real_price']-df_all['real_price_30max'])/df_all['real_price_30max']
        #df_all['30_pct_max_rank']=df_all.groupby('trade_date')['30_pct_max'].rank(pct=True)
        #df_all['30_pct_max_rank']=df_all['30_pct_max_rank']*10//2


        #60日最低比值
        xxx=df_all.groupby('ts_code')['real_price'].rolling(20).min().reset_index()
        xxx.set_index(['level_1'], drop=True, append=False, inplace=True, verify_integrity=False)
        xxx.drop(['ts_code'],axis=1,inplace=True)
        
        df_all=df_all.join(xxx, lsuffix='', rsuffix='_20min')
        #bbb=df_all.groupby('ts_code')['real_price'].agg({'all_min':np.min, 'all_max': np.max}).reset_index()
        #ccc=pd.merge(df_all, bbb, how='inner', on=['ts_code'])
        df_all['20_pct']=(df_all['real_price']-df_all['real_price_20min'])/df_all['real_price_20min']
        df_all['20_pct_rank']=df_all.groupby('trade_date')['20_pct'].rank(pct=True)
        df_all['20_pct_rank']=df_all['20_pct_rank']*10//2

        #20日最高比值
        xxx=df_all.groupby('ts_code')['real_price'].rolling(20).max().reset_index()
        xxx.set_index(['level_1'], drop=True, append=False, inplace=True, verify_integrity=False)
        xxx.drop(['ts_code'],axis=1,inplace=True)
        
        df_all=df_all.join(xxx, lsuffix='', rsuffix='_20max')
        #bbb=df_all.groupby('ts_code')['real_price'].agg({'all_min':np.min, 'all_max': np.max}).reset_index()
        #ccc=pd.merge(df_all, bbb, how='inner', on=['ts_code'])
        df_all['20_pct_max']=(df_all['real_price']-df_all['real_price_20max'])/df_all['real_price_20max']
        df_all['20_pct_max_rank']=df_all.groupby('trade_date')['20_pct_max'].rank(pct=True)
        df_all['20_pct_max_rank']=df_all['20_pct_max_rank']*10//2


        #df_all.drop(['30_pct','real_price_30min','30_pct_max','real_price_30max'],axis=1,inplace=True)
        df_all.drop(['20_pct','real_price_20min','20_pct_max','real_price_20max'],axis=1,inplace=True)

        df_all.drop(['change','vol'],axis=1,inplace=True)

        #===================================================================================================================================#
    
        #明日幅度
        tm1=df_all.groupby('ts_code')['pct_chg'].shift(-1)
        tm2=df_all.groupby('ts_code')['pct_chg'].shift(-2)
        #df_all['tomorrow_chg']=((100+tm1)*(100+tm2)-10000)/100
        tm3=df_all.groupby('ts_code')['pct_chg'].shift(-3)
        tm4=df_all.groupby('ts_code')['pct_chg'].shift(-4)
        tm5=df_all.groupby('ts_code')['pct_chg'].shift(-5)

        df_all['tomorrow_chg']=(((100+tm1)/100)*((100+tm2)/100)*((100+tm3)/100)*((100+tm4)/100)*((100+tm5)/100)-1)*100

        #df_all['tomorrow_chg']=((100+tm1)*(100+tm2)*(100+tm3)-1000000)/10000
        #df_all['tomorrow_chg']=df_all.groupby('ts_code')['pct_chg'].shift(-1)
        #明日排名
        df_all['tomorrow_chg_rank']=df_all.groupby('trade_date')['tomorrow_chg'].rank(pct=True)
        df_all['tomorrow_chg_rank']=df_all['tomorrow_chg_rank']*9.9//1
        #是否停
        df_all['high_stop']=0
        df_all.loc[df_all['pct_chg']>9.4,'high_stop']=1
        df_all.loc[(df_all['pct_chg']<5.2) & (4.8<df_all['pct_chg']),'high_stop']=1


        ###真实价格范围(区分实际股价高低)
        #df_all['price_real_rank']=df_all.groupby('trade_date')['pre_close'].rank(pct=True)
        #df_all['price_real_rank']=df_all['price_real_rank']*10//1
        #1日
        df_all['chg_rank']=df_all.groupby('trade_date')['pct_chg'].rank(pct=True)
        df_all['chg_rank']=df_all['chg_rank']*10//2


        #6日
        xxx=df_all.groupby('ts_code')['chg_rank'].rolling(6).sum().reset_index()
        xxx.set_index(['level_1'], drop=True, append=False, inplace=True, verify_integrity=False)
        xxx.drop(['ts_code'],axis=1,inplace=True)

        df_all=df_all.join(xxx, lsuffix='', rsuffix='_6')

        df_all['chg_rank_6']=df_all.groupby('trade_date')['chg_rank_6'].rank(pct=True)
        df_all['chg_rank_6']=df_all['chg_rank_6']*10//2

        #10日
        xxx=df_all.groupby('ts_code')['chg_rank'].rolling(10).sum().reset_index()
        xxx.set_index(['level_1'], drop=True, append=False, inplace=True, verify_integrity=False)
        xxx.drop(['ts_code'],axis=1,inplace=True)

        df_all=df_all.join(xxx, lsuffix='', rsuffix='_10')

        df_all['chg_rank_10']=df_all.groupby('trade_date')['chg_rank_10'].rank(pct=True)
        df_all['chg_rank_10']=df_all['chg_rank_10']*10//2

        #3日
        xxx=df_all.groupby('ts_code')['chg_rank'].rolling(3).sum().reset_index()
        xxx.set_index(['level_1'], drop=True, append=False, inplace=True, verify_integrity=False)
        xxx.drop(['ts_code'],axis=1,inplace=True)

        df_all=df_all.join(xxx, lsuffix='', rsuffix='_3')

        df_all['chg_rank_3']=df_all.groupby('trade_date')['chg_rank_3'].rank(pct=True)
        df_all['chg_rank_3']=df_all['chg_rank_3']*10//2

        ##20日
        #xxx=df_all.groupby('ts_code')['chg_rank'].rolling(20).sum().reset_index()
        #xxx.set_index(['level_1'], drop=True, append=False, inplace=True, verify_integrity=False)
        #xxx.drop(['ts_code'],axis=1,inplace=True)

        #df_all=df_all.join(xxx, lsuffix='', rsuffix='_20')

        #df_all['chg_rank_20']=df_all.groupby('trade_date')['chg_rank_3'].rank(pct=True)
        #df_all['chg_rank_20']=df_all['chg_rank_20']*10//1

        #print(df_all)

        #10日均量
        xxx=df_all.groupby('ts_code')['amount'].rolling(10).mean().reset_index()
        xxx.set_index(['level_1'], drop=True, append=False, inplace=True, verify_integrity=False)
        xxx.drop(['ts_code'],axis=1,inplace=True)
        df_all=df_all.join(xxx, lsuffix='', rsuffix='_10')

        #当日量占比
        df_all['pst_amount']=df_all['amount']/df_all['amount_10']
        df_all.drop(['amount_10'],axis=1,inplace=True)
        #当日量排名
        df_all['pst_amount_rank_10']=df_all.groupby('trade_date')['pst_amount'].rank(pct=True)
        df_all['pst_amount_rank_10']=df_all['pst_amount_rank_10']*10//2

        ##5日均量
        #xxx=df_all.groupby('ts_code')['amount'].rolling(5).mean().reset_index()
        #xxx.set_index(['level_1'], drop=True, append=False, inplace=True, verify_integrity=False)
        #xxx.drop(['ts_code'],axis=1,inplace=True)
        #df_all=df_all.join(xxx, lsuffix='', rsuffix='_5')

        ##当日量占比
        #df_all['pst_amount']=df_all['amount']/df_all['amount_5']
        #df_all.drop(['amount_5'],axis=1,inplace=True)
        ##当日量排名
        #df_all['pst_amount_rank_5']=df_all.groupby('trade_date')['pst_amount'].rank(pct=True)
        #df_all['pst_amount_rank_5']=df_all['pst_amount_rank_5']*10//1

        #计算三种比例rank
        dolist=['open','high','low']

        for curc in dolist:
            buffer=((df_all[curc]-df_all['pre_close'])*100)/df_all['close']
            df_all[curc]=buffer
            df_all[curc]=df_all.groupby('trade_date')[curc].rank(pct=True)
            df_all[curc]=df_all[curc]*10//2

        #加入昨日rank
        df_all['yesterday_open']=df_all.groupby('ts_code')['open'].shift(1)
        df_all['yesterday_high']=df_all.groupby('ts_code')['high'].shift(1)
        df_all['yesterday_low']=df_all.groupby('ts_code')['low'].shift(1)
        df_all['yesterday_pst_amount_rank']=df_all.groupby('ts_code')['pst_amount_rank_10'].shift(1)
        ##加入前日open
        df_all['yesterday2_open']=df_all.groupby('ts_code')['open'].shift(2)
        df_all['yesterday2_high']=df_all.groupby('ts_code')['high'].shift(2)
        df_all['yesterday2_low']=df_all.groupby('ts_code')['low'].shift(2)
        df_all['yesterday2_pst_amount_rank']=df_all.groupby('ts_code')['pst_amount_rank_10'].shift(2)
        ##加入前日open
        df_all['yesterday3_open']=df_all.groupby('ts_code')['open'].shift(3)
        df_all['yesterday3_high']=df_all.groupby('ts_code')['high'].shift(3)
        df_all['yesterday3_low']=df_all.groupby('ts_code')['low'].shift(3)
        df_all['yesterday3_pst_amount_rank']=df_all.groupby('ts_code')['pst_amount_rank_10'].shift(3)


        df_all.drop(['close','pre_close','pct_chg','pst_amount','adj_factor','real_price','amount'],axis=1,inplace=True)
        #df_all.drop(['close','pre_close','pct_chg','adj_factor','real_price'],axis=1,inplace=True)

        #暂时不用的列
        df_all=df_all[df_all['high_stop']==0]

        #'tomorrow_chg'
        df_all.drop(['high_stop'],axis=1,inplace=True) 

        df_all.dropna(axis=0,how='any',inplace=True)

        print(df_all)
        df_all=df_all.reset_index(drop=True)

        return df_all

    def real_FE(self):

        df_data=pd.read_csv('real_now.csv',index_col=0,header=0)
        df_adj_all=pd.read_csv('real_adj_now.csv',index_col=0,header=0)

        df_all=pd.merge(df_data, df_adj_all, how='left', on=['ts_code','trade_date'])

        #df_all=df_all[df_all['ts_code'].str.startswith('688')==False]

        #df_all=pd.read_csv(bufferstring,index_col=0,header=0,nrows=100000)
    
        #df_all.drop(['change','vol'],axis=1,inplace=True)
 

        #===================================================================================================================================#

        #复权后价格
        df_all['adj_factor']=df_all['adj_factor'].fillna(0)
        df_all['real_price']=df_all['close']*df_all['adj_factor']
        
        df_all['real_price']=df_all.groupby('ts_code')['real_price'].shift(1)
        df_all['real_price']=df_all['real_price']*(1+df_all['pct_chg']/100)


        #30日最低比值
        xxx=df_all.groupby('ts_code')['real_price'].rolling(30).min().reset_index()
        xxx.set_index(['level_1'], drop=True, append=False, inplace=True, verify_integrity=False)
        xxx.drop(['ts_code'],axis=1,inplace=True)
        
        df_all=df_all.join(xxx, lsuffix='', rsuffix='_30min')
        #bbb=df_all.groupby('ts_code')['real_price'].agg({'all_min':np.min, 'all_max': np.max}).reset_index()
        #ccc=pd.merge(df_all, bbb, how='inner', on=['ts_code'])
        df_all['30_pct']=(df_all['real_price']-df_all['real_price_30min'])/df_all['real_price_30min']
        df_all['30_pct_rank']=df_all.groupby('trade_date')['30_pct'].rank(pct=True)
        df_all['30_pct_rank']=df_all['30_pct_rank']*10//2

        #30日最高比值
        xxx=df_all.groupby('ts_code')['real_price'].rolling(30).max().reset_index()
        xxx.set_index(['level_1'], drop=True, append=False, inplace=True, verify_integrity=False)
        xxx.drop(['ts_code'],axis=1,inplace=True)
        
        df_all=df_all.join(xxx, lsuffix='', rsuffix='_30max')
        #bbb=df_all.groupby('ts_code')['real_price'].agg({'all_min':np.min, 'all_max': np.max}).reset_index()
        #ccc=pd.merge(df_all, bbb, how='inner', on=['ts_code'])
        df_all['30_pct_max']=(df_all['real_price']-df_all['real_price_30max'])/df_all['real_price_30max']
        df_all['30_pct_max_rank']=df_all.groupby('trade_date')['30_pct_max'].rank(pct=True)
        df_all['30_pct_max_rank']=df_all['30_pct_max_rank']*10//2

        df_all.drop(['30_pct','real_price_30min','30_pct_max','real_price_30max'],axis=1,inplace=True)


        #===================================================================================================================================#


        #是否停
        df_all['high_stop']=0
        df_all.loc[df_all['pct_chg']>9.7,'high_stop']=1
        df_all.loc[(df_all['pct_chg']<5.2) & (4.8<df_all['pct_chg']),'high_stop']=1


        #1日
        df_all['chg_rank']=df_all.groupby('trade_date')['pct_chg'].rank(pct=True)
        df_all['chg_rank']=df_all['chg_rank']*10//2


        #6日
        xxx=df_all.groupby('ts_code')['chg_rank'].rolling(6).sum().reset_index()
        xxx.set_index(['level_1'], drop=True, append=False, inplace=True, verify_integrity=False)
        xxx.drop(['ts_code'],axis=1,inplace=True)

        df_all=df_all.join(xxx, lsuffix='', rsuffix='_6')

        df_all['chg_rank_6']=df_all.groupby('trade_date')['chg_rank_6'].rank(pct=True)
        df_all['chg_rank_6']=df_all['chg_rank_6']*10//2

        #10日
        xxx=df_all.groupby('ts_code')['chg_rank'].rolling(10).sum().reset_index()
        xxx.set_index(['level_1'], drop=True, append=False, inplace=True, verify_integrity=False)
        xxx.drop(['ts_code'],axis=1,inplace=True)

        df_all=df_all.join(xxx, lsuffix='', rsuffix='_10')

        df_all['chg_rank_10']=df_all.groupby('trade_date')['chg_rank_10'].rank(pct=True)
        df_all['chg_rank_10']=df_all['chg_rank_10']*10//2

        #3日
        xxx=df_all.groupby('ts_code')['chg_rank'].rolling(3).sum().reset_index()
        xxx.set_index(['level_1'], drop=True, append=False, inplace=True, verify_integrity=False)
        xxx.drop(['ts_code'],axis=1,inplace=True)

        df_all=df_all.join(xxx, lsuffix='', rsuffix='_3')

        df_all['chg_rank_3']=df_all.groupby('trade_date')['chg_rank_3'].rank(pct=True)
        df_all['chg_rank_3']=df_all['chg_rank_3']*10//2

        #print(df_all)

        #10日均量
        xxx=df_all.groupby('ts_code')['amount'].rolling(10).mean().reset_index()
        xxx.set_index(['level_1'], drop=True, append=False, inplace=True, verify_integrity=False)
        xxx.drop(['ts_code'],axis=1,inplace=True)
        df_all=df_all.join(xxx, lsuffix='', rsuffix='_10')

        #当日量占比
        df_all['pst_amount']=df_all['amount']/df_all['amount_10']
        df_all.drop(['amount_10'],axis=1,inplace=True)
        #当日量排名
        df_all['pst_amount_rank_10']=df_all.groupby('trade_date')['pst_amount'].rank(pct=True)
        df_all['pst_amount_rank_10']=df_all['pst_amount_rank_10']*10//2

        #计算三种比例rank
        dolist=['open','high','low']

        for curc in dolist:
            buffer=((df_all[curc]-df_all['pre_close'])*100)/df_all['pre_close']
            df_all[curc]=buffer
            df_all[curc]=df_all.groupby('trade_date')[curc].rank(pct=True)
            df_all[curc]=df_all[curc]*10//2

        #加入昨日rank
        df_all['yesterday_open']=df_all.groupby('ts_code')['open'].shift(1)
        df_all['yesterday_high']=df_all.groupby('ts_code')['high'].shift(1)
        df_all['yesterday_low']=df_all.groupby('ts_code')['low'].shift(1)
        df_all['yesterday_pst_amount_rank']=df_all.groupby('ts_code')['pst_amount_rank_10'].shift(1)
        ##加入前日open
        df_all['yesterday2_open']=df_all.groupby('ts_code')['open'].shift(2)
        df_all['yesterday2_high']=df_all.groupby('ts_code')['high'].shift(2)
        df_all['yesterday2_low']=df_all.groupby('ts_code')['low'].shift(2)
        df_all['yesterday2_pst_amount_rank']=df_all.groupby('ts_code')['pst_amount_rank_10'].shift(2)
        ##加入前日open
        df_all['yesterday3_open']=df_all.groupby('ts_code')['open'].shift(3)
        df_all['yesterday3_high']=df_all.groupby('ts_code')['high'].shift(3)
        df_all['yesterday3_low']=df_all.groupby('ts_code')['low'].shift(3)
        df_all['yesterday3_pst_amount_rank']=df_all.groupby('ts_code')['pst_amount_rank_10'].shift(3)

        df_all.drop(['close','pre_close','pct_chg','pst_amount','adj_factor','real_price','amount'],axis=1,inplace=True)



        #暂时不用的列
        df_all=df_all[df_all['high_stop']==0]
        #'tomorrow_chg'
        df_all.drop(['high_stop'],axis=1,inplace=True)



        df_all.dropna(axis=0,how='any',inplace=True)


        month_sec=df_all['trade_date'].max()
        df_all=df_all[df_all['trade_date']!=month_sec]
        print(df_all)
        df_all=df_all.reset_index(drop=True)

        df_all.to_csv('today_train.csv')
        dwdw=1

class FEo30New2(FEbase):
    def __init__(self):
        pass
    def changerank(self,a,b):
        a.loc[-8>a[b],b]=-20   
        a.loc[(a[b]<-4) & (-8<=a[b]),b]=-19
        a.loc[(a[b]<-1.5) & (-4<=a[b]),b]=-18
        a.loc[(a[b]<-0.5) & (-1.5<=a[b]),b]=-17
        a.loc[(a[b]<0) & (-0.5<=a[b]),b]=-16
        a.loc[(a[b]<0.5) & (0<=a[b]),b]=-15
        a.loc[(a[b]<1.5) & (0.5<=a[b]),b]=-14
        a.loc[(a[b]<4) & (1.5<=a[b]),b]=-13
        a.loc[(a[b]<8) & (4<=a[b]),b]=-12
        a.loc[(a[b]>=8) ,b]=-11
        a[b]=a[b]+20
        return a[b]

    def core(self,DataSetName):

        df_data=pd.read_csv(DataSetName[0],index_col=0,header=0)
        df_adj_all=pd.read_csv(DataSetName[1],index_col=0,header=0)
        #df_long_all=pd.read_csv(DataSetName[2],index_col=0,header=0)

        df_all=pd.merge(df_data, df_adj_all, how='inner', on=['ts_code','trade_date'])
        #df_all=pd.merge(df_all, df_long_all, how='inner', on=['ts_code','trade_date'])


        ##排除科创版
        #print(df_all)
        df_all=df_all[df_all['ts_code'].str.startswith('688')==False]
        
        #===========================================================================================================
        ##1日
        #buffer=df_all.groupby('trade_date')['pct_chg'].mean()
        #df_all=pd.merge(df_all, buffer, how='left', on=['trade_date'],suffixes=('','_mean'))

        ##3日
        #xxx=df_all.groupby('ts_code')['pct_chg_mean'].rolling(3).sum().reset_index()
        #xxx.set_index(['level_1'], drop=True, append=False, inplace=True, verify_integrity=False)
        #xxx.drop(['ts_code'],axis=1,inplace=True)

        #df_all=df_all.join(xxx, lsuffix='', rsuffix='_3')

        ##5日
        #xxx=df_all.groupby('ts_code')['pct_chg_mean'].rolling(5).sum().reset_index()
        #xxx.set_index(['level_1'], drop=True, append=False, inplace=True, verify_integrity=False)
        #xxx.drop(['ts_code'],axis=1,inplace=True)

        #df_all=df_all.join(xxx, lsuffix='', rsuffix='_5')

        ##10日
        #xxx=df_all.groupby('ts_code')['pct_chg_mean'].rolling(10).sum().reset_index()
        #xxx.set_index(['level_1'], drop=True, append=False, inplace=True, verify_integrity=False)
        #xxx.drop(['ts_code'],axis=1,inplace=True)

        #df_all=df_all.join(xxx, lsuffix='', rsuffix='_10')

        #df_all.loc[df_all['pct_chg_mean']>=0,'pct_chg_mean']=1
        #df_all.loc[df_all['pct_chg_mean']<0,'pct_chg_mean']=-1
        #df_all.loc[df_all['pct_chg_mean_3']>=0,'pct_chg_mean_3']=1
        #df_all.loc[df_all['pct_chg_mean_3']<0,'pct_chg_mean_3']=-1
        #df_all.loc[df_all['pct_chg_mean_5']>=0,'pct_chg_mean_5']=1
        #df_all.loc[df_all['pct_chg_mean_5']<0,'pct_chg_mean_5']=-1
        #df_all.loc[df_all['pct_chg_mean_10']>=0,'pct_chg_mean_10']=1
        #df_all.loc[df_all['pct_chg_mean_10']<0,'pct_chg_mean_10']=-1

        #===========================================================================================================

        #复权后价格
        df_all['real_price']=df_all['close']*df_all['adj_factor']
        
        #30日最低比值
        xxx=df_all.groupby('ts_code')['real_price'].rolling(30).min().reset_index()
        xxx.set_index(['level_1'], drop=True, append=False, inplace=True, verify_integrity=False)
        xxx.drop(['ts_code'],axis=1,inplace=True)
        
        df_all=df_all.join(xxx, lsuffix='', rsuffix='_30min')
        #bbb=df_all.groupby('ts_code')['real_price'].agg({'all_min':np.min, 'all_max': np.max}).reset_index()
        #ccc=pd.merge(df_all, bbb, how='inner', on=['ts_code'])
        df_all['30_pct']=(df_all['real_price']-df_all['real_price_30min'])/df_all['real_price_30min']
        df_all['30_pct_rank']=df_all.groupby('trade_date')['30_pct'].rank(pct=True)
        df_all['30_pct_rank']=df_all['30_pct_rank']*10//2

        #30日最高比值
        xxx=df_all.groupby('ts_code')['real_price'].rolling(30).max().reset_index()
        xxx.set_index(['level_1'], drop=True, append=False, inplace=True, verify_integrity=False)
        xxx.drop(['ts_code'],axis=1,inplace=True)
        
        df_all=df_all.join(xxx, lsuffix='', rsuffix='_30max')
        #bbb=df_all.groupby('ts_code')['real_price'].agg({'all_min':np.min, 'all_max': np.max}).reset_index()
        #ccc=pd.merge(df_all, bbb, how='inner', on=['ts_code'])
        df_all['30_pct_max']=(df_all['real_price']-df_all['real_price_30max'])/df_all['real_price_30max']
        df_all['30_pct_max_rank']=df_all.groupby('trade_date')['30_pct_max'].rank(pct=True)
        df_all['30_pct_max_rank']=df_all['30_pct_max_rank']*10//2



        df_all.drop(['30_pct','real_price_30min','30_pct_max','real_price_30max'],axis=1,inplace=True)
        #df_all.drop(['20_pct','real_price_20min','20_pct_max','real_price_20max'],axis=1,inplace=True)

        df_all.drop(['change','vol'],axis=1,inplace=True)

        #===================================================================================================================================#
    
        ##明日幅度
        #tm1=df_all.groupby('ts_code')['pct_chg'].shift(-1)
        #tm2=df_all.groupby('ts_code')['pct_chg'].shift(-2)
        ##df_all['tomorrow_chg']=((100+tm1)*(100+tm2)-10000)/100
        #tm3=df_all.groupby('ts_code')['pct_chg'].shift(-3)
        #tm4=df_all.groupby('ts_code')['pct_chg'].shift(-4)
        #tm5=df_all.groupby('ts_code')['pct_chg'].shift(-5)

        #df_all['tomorrow_chg']=(((100+tm1)/100)*((100+tm2)/100)*((100+tm3)/100)*((100+tm4)/100)*((100+tm5)/100)-1)*100

        #df_all['tomorrow_chg']=((100+tm1)*(100+tm2)*(100+tm3)-1000000)/10000
        df_all['tomorrow_chg']=df_all.groupby('ts_code')['pct_chg'].shift(-1)
        #明日排名
        #df_all['tomorrow_chg_rank']=df_all.groupby('trade_date')['tomorrow_chg'].rank(pct=True)
        #df_all['tomorrow_chg_rank']=df_all['tomorrow_chg_rank']*9.9//1

        df_all['tomorrow_chg_rank']=df_all['tomorrow_chg']      

        df_all['tomorrow_chg_rank']=self.changerank(df_all,'tomorrow_chg_rank')

        #print(df_all)

        #是否停
        df_all['high_stop']=0
        df_all.loc[df_all['pct_chg']>9.4,'high_stop']=1
        df_all.loc[(df_all['pct_chg']<5.2) & (4.8<df_all['pct_chg']),'high_stop']=1


        ###真实价格范围(区分实际股价高低)
        #df_all['price_real_rank']=df_all.groupby('trade_date')['pre_close'].rank(pct=True)
        #df_all['price_real_rank']=df_all['price_real_rank']*10//1
        #1日
        df_all['chg_rank']=df_all.groupby('trade_date')['pct_chg'].rank(pct=True)
        #df_all['chg_rank']=df_all['chg_rank']*10//2

        df_all['chg_rank']=self.changerank(df_all,'chg_rank')

        #6日
        xxx=df_all.groupby('ts_code')['chg_rank'].rolling(6).sum().reset_index()
        xxx.set_index(['level_1'], drop=True, append=False, inplace=True, verify_integrity=False)
        xxx.drop(['ts_code'],axis=1,inplace=True)

        df_all=df_all.join(xxx, lsuffix='', rsuffix='_6')

        df_all['chg_rank_6']=df_all.groupby('trade_date')['chg_rank_6'].rank(pct=True)
        df_all['chg_rank_6']=df_all['chg_rank_6']*10//2

        #10日
        xxx=df_all.groupby('ts_code')['chg_rank'].rolling(10).sum().reset_index()
        xxx.set_index(['level_1'], drop=True, append=False, inplace=True, verify_integrity=False)
        xxx.drop(['ts_code'],axis=1,inplace=True)

        df_all=df_all.join(xxx, lsuffix='', rsuffix='_10')

        df_all['chg_rank_10']=df_all.groupby('trade_date')['chg_rank_10'].rank(pct=True)
        df_all['chg_rank_10']=df_all['chg_rank_10']*10//2

        #3日
        xxx=df_all.groupby('ts_code')['chg_rank'].rolling(3).sum().reset_index()
        xxx.set_index(['level_1'], drop=True, append=False, inplace=True, verify_integrity=False)
        xxx.drop(['ts_code'],axis=1,inplace=True)

        df_all=df_all.join(xxx, lsuffix='', rsuffix='_3')

        df_all['chg_rank_3']=df_all.groupby('trade_date')['chg_rank_3'].rank(pct=True)
        df_all['chg_rank_3']=df_all['chg_rank_3']*10//2

        ##20日
        #xxx=df_all.groupby('ts_code')['chg_rank'].rolling(20).sum().reset_index()
        #xxx.set_index(['level_1'], drop=True, append=False, inplace=True, verify_integrity=False)
        #xxx.drop(['ts_code'],axis=1,inplace=True)

        #df_all=df_all.join(xxx, lsuffix='', rsuffix='_20')

        #df_all['chg_rank_20']=df_all.groupby('trade_date')['chg_rank_3'].rank(pct=True)
        #df_all['chg_rank_20']=df_all['chg_rank_20']*10//1

        #print(df_all)

        #10日均量
        xxx=df_all.groupby('ts_code')['amount'].rolling(10).mean().reset_index()
        xxx.set_index(['level_1'], drop=True, append=False, inplace=True, verify_integrity=False)
        xxx.drop(['ts_code'],axis=1,inplace=True)
        df_all=df_all.join(xxx, lsuffix='', rsuffix='_10')

        #当日量占比
        df_all['pst_amount']=df_all['amount']/df_all['amount_10']
        df_all.drop(['amount_10'],axis=1,inplace=True)
        #当日量排名
        df_all['pst_amount_rank_10']=df_all.groupby('trade_date')['pst_amount'].rank(pct=True)
        df_all['pst_amount_rank_10']=df_all['pst_amount_rank_10']*10//2

        ##5日均量
        #xxx=df_all.groupby('ts_code')['amount'].rolling(5).mean().reset_index()
        #xxx.set_index(['level_1'], drop=True, append=False, inplace=True, verify_integrity=False)
        #xxx.drop(['ts_code'],axis=1,inplace=True)
        #df_all=df_all.join(xxx, lsuffix='', rsuffix='_5')

        ##当日量占比
        #df_all['pst_amount']=df_all['amount']/df_all['amount_5']
        #df_all.drop(['amount_5'],axis=1,inplace=True)
        ##当日量排名
        #df_all['pst_amount_rank_5']=df_all.groupby('trade_date')['pst_amount'].rank(pct=True)
        #df_all['pst_amount_rank_5']=df_all['pst_amount_rank_5']*10//1

        #计算三种比例rank
        dolist=['open','high','low']

        for curc in dolist:
            buffer=((df_all[curc]-df_all['pre_close'])*100)/df_all['close']
            df_all[curc]=buffer
            df_all[curc]=df_all.groupby('trade_date')[curc].rank(pct=True)
            df_all[curc]=df_all[curc]*10//2

        #加入昨日rank
        df_all['yesterday_open']=df_all.groupby('ts_code')['open'].shift(1)
        df_all['yesterday_high']=df_all.groupby('ts_code')['high'].shift(1)
        df_all['yesterday_low']=df_all.groupby('ts_code')['low'].shift(1)
        df_all['yesterday_pst_amount_rank']=df_all.groupby('ts_code')['pst_amount_rank_10'].shift(1)
        ##加入前日open
        df_all['yesterday2_open']=df_all.groupby('ts_code')['open'].shift(2)
        df_all['yesterday2_high']=df_all.groupby('ts_code')['high'].shift(2)
        df_all['yesterday2_low']=df_all.groupby('ts_code')['low'].shift(2)
        df_all['yesterday2_pst_amount_rank']=df_all.groupby('ts_code')['pst_amount_rank_10'].shift(2)
        ##加入前日open
        df_all['yesterday3_open']=df_all.groupby('ts_code')['open'].shift(3)
        df_all['yesterday3_high']=df_all.groupby('ts_code')['high'].shift(3)
        df_all['yesterday3_low']=df_all.groupby('ts_code')['low'].shift(3)
        df_all['yesterday3_pst_amount_rank']=df_all.groupby('ts_code')['pst_amount_rank_10'].shift(3)


        df_all.drop(['close','pre_close','pct_chg','pst_amount','adj_factor','real_price','amount'],axis=1,inplace=True)
        #df_all.drop(['close','pre_close','pct_chg','adj_factor','real_price'],axis=1,inplace=True)

        #暂时不用的列
        df_all=df_all[df_all['high_stop']==0]

        #'tomorrow_chg'
        df_all.drop(['high_stop'],axis=1,inplace=True) 

        df_all.dropna(axis=0,how='any',inplace=True)

        print(df_all)
        df_all=df_all.reset_index(drop=True)

        return df_all

    def real_FE(self):

        df_data=pd.read_csv('real_now.csv',index_col=0,header=0)
        df_adj_all=pd.read_csv('real_adj_now.csv',index_col=0,header=0)

        df_all=pd.merge(df_data, df_adj_all, how='left', on=['ts_code','trade_date'])

        #df_all=df_all[df_all['ts_code'].str.startswith('688')==False]

        #df_all=pd.read_csv(bufferstring,index_col=0,header=0,nrows=100000)
    
        #df_all.drop(['change','vol'],axis=1,inplace=True)
 

        #===================================================================================================================================#
        
        #复权后价格
        df_all['adj_factor']=df_all['adj_factor'].fillna(0)
        df_all['real_price']=df_all['close']*df_all['adj_factor']
        
        df_all['real_price']=df_all.groupby('ts_code')['real_price'].shift(1)
        df_all['real_price']=df_all['real_price']*(1+df_all['pct_chg']/100)


        #30日最低比值
        xxx=df_all.groupby('ts_code')['real_price'].rolling(30).min().reset_index()
        xxx.set_index(['level_1'], drop=True, append=False, inplace=True, verify_integrity=False)
        xxx.drop(['ts_code'],axis=1,inplace=True)
        
        df_all=df_all.join(xxx, lsuffix='', rsuffix='_30min')
        #bbb=df_all.groupby('ts_code')['real_price'].agg({'all_min':np.min, 'all_max': np.max}).reset_index()
        #ccc=pd.merge(df_all, bbb, how='inner', on=['ts_code'])
        df_all['30_pct']=(df_all['real_price']-df_all['real_price_30min'])/df_all['real_price_30min']
        df_all['30_pct_rank']=df_all.groupby('trade_date')['30_pct'].rank(pct=True)
        df_all['30_pct_rank']=df_all['30_pct_rank']*10//2

        #30日最高比值
        xxx=df_all.groupby('ts_code')['real_price'].rolling(30).max().reset_index()
        xxx.set_index(['level_1'], drop=True, append=False, inplace=True, verify_integrity=False)
        xxx.drop(['ts_code'],axis=1,inplace=True)
        
        df_all=df_all.join(xxx, lsuffix='', rsuffix='_30max')
        #bbb=df_all.groupby('ts_code')['real_price'].agg({'all_min':np.min, 'all_max': np.max}).reset_index()
        #ccc=pd.merge(df_all, bbb, how='inner', on=['ts_code'])
        df_all['30_pct_max']=(df_all['real_price']-df_all['real_price_30max'])/df_all['real_price_30max']
        df_all['30_pct_max_rank']=df_all.groupby('trade_date')['30_pct_max'].rank(pct=True)
        df_all['30_pct_max_rank']=df_all['30_pct_max_rank']*10//2

        df_all.drop(['30_pct','real_price_30min','30_pct_max','real_price_30max'],axis=1,inplace=True)


        #===================================================================================================================================#


        #是否停
        df_all['high_stop']=0
        df_all.loc[df_all['pct_chg']>9.7,'high_stop']=1
        df_all.loc[(df_all['pct_chg']<5.2) & (4.8<df_all['pct_chg']),'high_stop']=1


        #1日
        df_all['chg_rank']=df_all.groupby('trade_date')['pct_chg'].rank(pct=True)
        df_all['chg_rank']=df_all['chg_rank']*10//2


        #6日
        xxx=df_all.groupby('ts_code')['chg_rank'].rolling(6).sum().reset_index()
        xxx.set_index(['level_1'], drop=True, append=False, inplace=True, verify_integrity=False)
        xxx.drop(['ts_code'],axis=1,inplace=True)

        df_all=df_all.join(xxx, lsuffix='', rsuffix='_6')

        df_all['chg_rank_6']=df_all.groupby('trade_date')['chg_rank_6'].rank(pct=True)
        df_all['chg_rank_6']=df_all['chg_rank_6']*10//2

        #10日
        xxx=df_all.groupby('ts_code')['chg_rank'].rolling(10).sum().reset_index()
        xxx.set_index(['level_1'], drop=True, append=False, inplace=True, verify_integrity=False)
        xxx.drop(['ts_code'],axis=1,inplace=True)

        df_all=df_all.join(xxx, lsuffix='', rsuffix='_10')

        df_all['chg_rank_10']=df_all.groupby('trade_date')['chg_rank_10'].rank(pct=True)
        df_all['chg_rank_10']=df_all['chg_rank_10']*10//2

        #3日
        xxx=df_all.groupby('ts_code')['chg_rank'].rolling(3).sum().reset_index()
        xxx.set_index(['level_1'], drop=True, append=False, inplace=True, verify_integrity=False)
        xxx.drop(['ts_code'],axis=1,inplace=True)

        df_all=df_all.join(xxx, lsuffix='', rsuffix='_3')

        df_all['chg_rank_3']=df_all.groupby('trade_date')['chg_rank_3'].rank(pct=True)
        df_all['chg_rank_3']=df_all['chg_rank_3']*10//2

        #print(df_all)

        #10日均量
        xxx=df_all.groupby('ts_code')['amount'].rolling(10).mean().reset_index()
        xxx.set_index(['level_1'], drop=True, append=False, inplace=True, verify_integrity=False)
        xxx.drop(['ts_code'],axis=1,inplace=True)
        df_all=df_all.join(xxx, lsuffix='', rsuffix='_10')

        #当日量占比
        df_all['pst_amount']=df_all['amount']/df_all['amount_10']
        df_all.drop(['amount_10'],axis=1,inplace=True)
        #当日量排名
        df_all['pst_amount_rank_10']=df_all.groupby('trade_date')['pst_amount'].rank(pct=True)
        df_all['pst_amount_rank_10']=df_all['pst_amount_rank_10']*10//2

        #计算三种比例rank
        dolist=['open','high','low']

        for curc in dolist:
            buffer=((df_all[curc]-df_all['pre_close'])*100)/df_all['pre_close']
            df_all[curc]=buffer
            df_all[curc]=df_all.groupby('trade_date')[curc].rank(pct=True)
            df_all[curc]=df_all[curc]*10//2

        #加入昨日rank
        df_all['yesterday_open']=df_all.groupby('ts_code')['open'].shift(1)
        df_all['yesterday_high']=df_all.groupby('ts_code')['high'].shift(1)
        df_all['yesterday_low']=df_all.groupby('ts_code')['low'].shift(1)
        df_all['yesterday_pst_amount_rank']=df_all.groupby('ts_code')['pst_amount_rank_10'].shift(1)
        ##加入前日open
        df_all['yesterday2_open']=df_all.groupby('ts_code')['open'].shift(2)
        df_all['yesterday2_high']=df_all.groupby('ts_code')['high'].shift(2)
        df_all['yesterday2_low']=df_all.groupby('ts_code')['low'].shift(2)
        df_all['yesterday2_pst_amount_rank']=df_all.groupby('ts_code')['pst_amount_rank_10'].shift(2)
        ##加入前日open
        df_all['yesterday3_open']=df_all.groupby('ts_code')['open'].shift(3)
        df_all['yesterday3_high']=df_all.groupby('ts_code')['high'].shift(3)
        df_all['yesterday3_low']=df_all.groupby('ts_code')['low'].shift(3)
        df_all['yesterday3_pst_amount_rank']=df_all.groupby('ts_code')['pst_amount_rank_10'].shift(3)

        df_all.drop(['close','pre_close','pct_chg','pst_amount','adj_factor','real_price','amount'],axis=1,inplace=True)



        #暂时不用的列
        df_all=df_all[df_all['high_stop']==0]
        #'tomorrow_chg'
        df_all.drop(['high_stop'],axis=1,inplace=True)



        df_all.dropna(axis=0,how='any',inplace=True)


        month_sec=df_all['trade_date'].max()
        df_all=df_all[df_all['trade_date']==month_sec]
        print(df_all)
        df_all=df_all.reset_index(drop=True)

        df_all.to_csv('today_train.csv')
        dwdw=1

class FEg30(FEbase):
    def __init__(self):
        pass

    def core(self,DataSetName):

        df_data=pd.read_csv(DataSetName[0],index_col=0,header=0)
        df_adj_all=pd.read_csv(DataSetName[1],index_col=0,header=0)
        #df_long_all=pd.read_csv(DataSetName[2],index_col=0,header=0)

        df_all=pd.merge(df_data, df_adj_all, how='inner', on=['ts_code','trade_date'])
        #df_all=pd.merge(df_all, df_long_all, how='inner', on=['ts_code','trade_date'])

        #===================================================================================================================================#
        #df_all['pe'] = df_all['pe'].fillna(9999)
        #df_all['pb'] = df_all['pb'].fillna(9999)

        #df_all['pe_rank']=df_all.groupby('trade_date')['pe'].rank(pct=True)
        #df_all['pb_rank']=df_all.groupby('trade_date')['pb'].rank(pct=True)        
        #df_all['pe_rank']=df_all['pe_rank']*10//1
        #df_all['pb_rank']=df_all['pb_rank']*10//1

        #df_all.drop(['turnover_rate','volume_ratio','pe','pb'],axis=1,inplace=True)

        #print(df_all)
        #df_all.to_csv('sjefosia.csv')

        #===================================================================================================================================#

        ##排除科创版
        #print(df_all)
        df_all=df_all[df_all['ts_code'].str.startswith('688')==False]
        

        ##加入gap_day特征
        #start=df_all['trade_date'].apply(str)[0]
        #end=df_all['trade_date'].apply(str)[df_all.shape[0]-1]
        #xxx=pd.date_range(start,end)

        #df = pd.DataFrame(xxx)
        #df.columns = ['trade_date']
        #df['trade_date']=df['trade_date'].map(str).map(lambda x : x[:4]+x[5:7]+x[8:10]).astype("int64")

        #yyy=df_all['trade_date']
        #zzz2=yyy.unique()
        #df_2=pd.DataFrame(zzz2)
        #df_2.columns = ['trade_date']
        #df_2['day_flag']=1
    
        #result = pd.merge(df, df_2, how='left', on=['trade_date'])
        #result['day_flag2']=result['day_flag'].shift(-1)
        #result['gap_day']=0

        #result.loc[(result['day_flag']==1) & (result['day_flag2']!=1),'gap_day']=1

        #result.drop(['day_flag','day_flag2'],axis=1,inplace=True)

        #df_all=pd.merge(df_all, result, how='left', on=['trade_date'])

        #===================================================================================================================================#

        #复权后价格
        df_all['real_price']=df_all['close']*df_all['adj_factor']
        
        #30日最低比值
        xxx=df_all.groupby('ts_code')['real_price'].rolling(30).min().reset_index()
        xxx.set_index(['level_1'], drop=True, append=False, inplace=True, verify_integrity=False)
        xxx.drop(['ts_code'],axis=1,inplace=True)
        
        df_all=df_all.join(xxx, lsuffix='', rsuffix='_30min')
        #bbb=df_all.groupby('ts_code')['real_price'].agg({'all_min':np.min, 'all_max': np.max}).reset_index()
        #ccc=pd.merge(df_all, bbb, how='inner', on=['ts_code'])
        df_all['30_pct']=(df_all['real_price']-df_all['real_price_30min'])/df_all['real_price_30min']
        df_all['30_pct_rank']=df_all.groupby('trade_date')['30_pct'].rank(pct=True)
        df_all['30_pct_rank']=df_all['30_pct_rank']*10//2

        #30日最高比值
        xxx=df_all.groupby('ts_code')['real_price'].rolling(30).max().reset_index()
        xxx.set_index(['level_1'], drop=True, append=False, inplace=True, verify_integrity=False)
        xxx.drop(['ts_code'],axis=1,inplace=True)
        
        df_all=df_all.join(xxx, lsuffix='', rsuffix='_30max')
        #bbb=df_all.groupby('ts_code')['real_price'].agg({'all_min':np.min, 'all_max': np.max}).reset_index()
        #ccc=pd.merge(df_all, bbb, how='inner', on=['ts_code'])
        df_all['30_pct_max']=(df_all['real_price']-df_all['real_price_30max'])/df_all['real_price_30max']
        df_all['30_pct_max_rank']=df_all.groupby('trade_date')['30_pct_max'].rank(pct=True)
        df_all['30_pct_max_rank']=df_all['30_pct_max_rank']*10//2



        df_all.drop(['30_pct','real_price_30min','30_pct_max','real_price_30max'],axis=1,inplace=True)
        #df_all.drop(['20_pct','real_price_20min','20_pct_max','real_price_20max'],axis=1,inplace=True)

        df_all.drop(['change','vol'],axis=1,inplace=True)

        #===================================================================================================================================#
    
        #明日幅度
        tm1=df_all.groupby('ts_code')['pct_chg'].shift(-1)
        tm2=df_all.groupby('ts_code')['pct_chg'].shift(-2)
        #df_all['tomorrow_chg']=((100+tm1)*(100+tm2)-10000)/100
        tm3=df_all.groupby('ts_code')['pct_chg'].shift(-3)
        tm4=df_all.groupby('ts_code')['pct_chg'].shift(-4)
        tm5=df_all.groupby('ts_code')['pct_chg'].shift(-5)

        df_all['tomorrow_chg']=(((100+tm1)/100)*((100+tm2)/100)*((100+tm3)/100)*((100+tm4)/100)*((100+tm5)/100)-1)*100

        #df_all['tomorrow_chg']=((100+tm1)*(100+tm2)*(100+tm3)-1000000)/10000
        #df_all['tomorrow_chg']=df_all.groupby('ts_code')['pct_chg'].shift(-1)
        #明日排名
        df_all['tomorrow_chg_rank']=df_all.groupby('trade_date')['tomorrow_chg'].rank(pct=True)
        df_all['tomorrow_chg_rank']=df_all['tomorrow_chg_rank']*9.9//1
        #是否停
        df_all['high_stop']=0
        df_all.loc[df_all['pct_chg']>9.4,'high_stop']=1
        df_all.loc[(df_all['pct_chg']<5.2) & (4.8<df_all['pct_chg']),'high_stop']=1


        ###真实价格范围(区分实际股价高低)
        #df_all['price_real_rank']=df_all.groupby('trade_date')['pre_close'].rank(pct=True)
        #df_all['price_real_rank']=df_all['price_real_rank']*10//1
        #1日
        df_all['chg_rank']=df_all.groupby('trade_date')['pct_chg'].rank(pct=True)
        df_all['chg_rank']=df_all['chg_rank']*10//2


        #6日
        xxx=df_all.groupby('ts_code')['chg_rank'].rolling(6).sum().reset_index()
        xxx.set_index(['level_1'], drop=True, append=False, inplace=True, verify_integrity=False)
        xxx.drop(['ts_code'],axis=1,inplace=True)

        df_all=df_all.join(xxx, lsuffix='', rsuffix='_6')

        df_all['chg_rank_6']=df_all.groupby('trade_date')['chg_rank_6'].rank(pct=True)
        df_all['chg_rank_6']=df_all['chg_rank_6']*10//2

        #10日
        xxx=df_all.groupby('ts_code')['chg_rank'].rolling(10).sum().reset_index()
        xxx.set_index(['level_1'], drop=True, append=False, inplace=True, verify_integrity=False)
        xxx.drop(['ts_code'],axis=1,inplace=True)

        df_all=df_all.join(xxx, lsuffix='', rsuffix='_10')

        df_all['chg_rank_10']=df_all.groupby('trade_date')['chg_rank_10'].rank(pct=True)
        df_all['chg_rank_10']=df_all['chg_rank_10']*10//2

        #3日
        xxx=df_all.groupby('ts_code')['chg_rank'].rolling(3).sum().reset_index()
        xxx.set_index(['level_1'], drop=True, append=False, inplace=True, verify_integrity=False)
        xxx.drop(['ts_code'],axis=1,inplace=True)

        df_all=df_all.join(xxx, lsuffix='', rsuffix='_3')

        df_all['chg_rank_3']=df_all.groupby('trade_date')['chg_rank_3'].rank(pct=True)
        df_all['chg_rank_3']=df_all['chg_rank_3']*10//2

        ##20日
        #xxx=df_all.groupby('ts_code')['chg_rank'].rolling(20).sum().reset_index()
        #xxx.set_index(['level_1'], drop=True, append=False, inplace=True, verify_integrity=False)
        #xxx.drop(['ts_code'],axis=1,inplace=True)

        #df_all=df_all.join(xxx, lsuffix='', rsuffix='_20')

        #df_all['chg_rank_20']=df_all.groupby('trade_date')['chg_rank_3'].rank(pct=True)
        #df_all['chg_rank_20']=df_all['chg_rank_20']*10//1

        #print(df_all)

        #10日均量
        xxx=df_all.groupby('ts_code')['amount'].rolling(10).mean().reset_index()
        xxx.set_index(['level_1'], drop=True, append=False, inplace=True, verify_integrity=False)
        xxx.drop(['ts_code'],axis=1,inplace=True)
        df_all=df_all.join(xxx, lsuffix='', rsuffix='_10')

        #当日量占比
        df_all['pst_amount']=df_all['amount']/df_all['amount_10']
        df_all.drop(['amount_10'],axis=1,inplace=True)
        #当日量排名
        df_all['pst_amount_rank_10']=df_all.groupby('trade_date')['pst_amount'].rank(pct=True)
        df_all['pst_amount_rank_10']=df_all['pst_amount_rank_10']*10//2

        ##5日均量
        #xxx=df_all.groupby('ts_code')['amount'].rolling(5).mean().reset_index()
        #xxx.set_index(['level_1'], drop=True, append=False, inplace=True, verify_integrity=False)
        #xxx.drop(['ts_code'],axis=1,inplace=True)
        #df_all=df_all.join(xxx, lsuffix='', rsuffix='_5')

        ##当日量占比
        #df_all['pst_amount']=df_all['amount']/df_all['amount_5']
        #df_all.drop(['amount_5'],axis=1,inplace=True)
        ##当日量排名
        #df_all['pst_amount_rank_5']=df_all.groupby('trade_date')['pst_amount'].rank(pct=True)
        #df_all['pst_amount_rank_5']=df_all['pst_amount_rank_5']*10//1

        #计算三种比例rank
        dolist=['open','high','low']

        for curc in dolist:
            buffer=((df_all[curc]-df_all['pre_close'])*100)/df_all['close']
            df_all[curc]=buffer
            df_all[curc]=df_all.groupby('trade_date')[curc].rank(pct=True)
            df_all[curc]=df_all[curc]*10//2

        #加入昨日rank
        df_all['yesterday_open']=df_all.groupby('ts_code')['open'].shift(1)
        df_all['yesterday_high']=df_all.groupby('ts_code')['high'].shift(1)
        df_all['yesterday_low']=df_all.groupby('ts_code')['low'].shift(1)
        df_all['yesterday_pst_amount_rank']=df_all.groupby('ts_code')['pst_amount_rank_10'].shift(1)
        ##加入前日open
        df_all['yesterday2_open']=df_all.groupby('ts_code')['open'].shift(2)
        df_all['yesterday2_high']=df_all.groupby('ts_code')['high'].shift(2)
        df_all['yesterday2_low']=df_all.groupby('ts_code')['low'].shift(2)
        df_all['yesterday2_pst_amount_rank']=df_all.groupby('ts_code')['pst_amount_rank_10'].shift(2)
        ##加入前日open
        df_all['yesterday3_open']=df_all.groupby('ts_code')['open'].shift(3)
        df_all['yesterday3_high']=df_all.groupby('ts_code')['high'].shift(3)
        df_all['yesterday3_low']=df_all.groupby('ts_code')['low'].shift(3)
        df_all['yesterday3_pst_amount_rank']=df_all.groupby('ts_code')['pst_amount_rank_10'].shift(3)


        df_all.drop(['close','pre_close','pct_chg','pst_amount','adj_factor','real_price','amount'],axis=1,inplace=True)
        #df_all.drop(['close','pre_close','pct_chg','adj_factor','real_price'],axis=1,inplace=True)

        #暂时不用的列
        df_all=df_all[df_all['high_stop']==0]

        #'tomorrow_chg'
        df_all.drop(['high_stop'],axis=1,inplace=True) 

        df_all.dropna(axis=0,how='any',inplace=True)

        print(df_all)
        df_all=df_all.reset_index(drop=True)

        return df_all

    def real_FE(self):

        df_data=pd.read_csv('real_now.csv',index_col=0,header=0)
        df_adj_all=pd.read_csv('real_adj_now.csv',index_col=0,header=0)

        df_all=pd.merge(df_data, df_adj_all, how='left', on=['ts_code','trade_date'])

        #df_all=df_all[df_all['ts_code'].str.startswith('688')==False]

        #df_all=pd.read_csv(bufferstring,index_col=0,header=0,nrows=100000)
    
        #df_all.drop(['change','vol'],axis=1,inplace=True)
 

        #===================================================================================================================================#
        
        #复权后价格
        df_all['adj_factor']=df_all['adj_factor'].fillna(0)
        df_all['real_price']=df_all['close']*df_all['adj_factor']
        
        df_all['real_price']=df_all.groupby('ts_code')['real_price'].shift(1)
        df_all['real_price']=df_all['real_price']*(1+df_all['pct_chg']/100)


        #30日最低比值
        xxx=df_all.groupby('ts_code')['real_price'].rolling(30).min().reset_index()
        xxx.set_index(['level_1'], drop=True, append=False, inplace=True, verify_integrity=False)
        xxx.drop(['ts_code'],axis=1,inplace=True)
        
        df_all=df_all.join(xxx, lsuffix='', rsuffix='_30min')
        #bbb=df_all.groupby('ts_code')['real_price'].agg({'all_min':np.min, 'all_max': np.max}).reset_index()
        #ccc=pd.merge(df_all, bbb, how='inner', on=['ts_code'])
        df_all['30_pct']=(df_all['real_price']-df_all['real_price_30min'])/df_all['real_price_30min']
        df_all['30_pct_rank']=df_all.groupby('trade_date')['30_pct'].rank(pct=True)
        df_all['30_pct_rank']=df_all['30_pct_rank']*10//2

        #30日最高比值
        xxx=df_all.groupby('ts_code')['real_price'].rolling(30).max().reset_index()
        xxx.set_index(['level_1'], drop=True, append=False, inplace=True, verify_integrity=False)
        xxx.drop(['ts_code'],axis=1,inplace=True)
        
        df_all=df_all.join(xxx, lsuffix='', rsuffix='_30max')
        #bbb=df_all.groupby('ts_code')['real_price'].agg({'all_min':np.min, 'all_max': np.max}).reset_index()
        #ccc=pd.merge(df_all, bbb, how='inner', on=['ts_code'])
        df_all['30_pct_max']=(df_all['real_price']-df_all['real_price_30max'])/df_all['real_price_30max']
        df_all['30_pct_max_rank']=df_all.groupby('trade_date')['30_pct_max'].rank(pct=True)
        df_all['30_pct_max_rank']=df_all['30_pct_max_rank']*10//2

        df_all.drop(['30_pct','real_price_30min','30_pct_max','real_price_30max'],axis=1,inplace=True)


        #===================================================================================================================================#


        #是否停
        df_all['high_stop']=0
        df_all.loc[df_all['pct_chg']>9.7,'high_stop']=1
        df_all.loc[(df_all['pct_chg']<5.2) & (4.8<df_all['pct_chg']),'high_stop']=1


        #1日
        df_all['chg_rank']=df_all.groupby('trade_date')['pct_chg'].rank(pct=True)
        df_all['chg_rank']=df_all['chg_rank']*10//2


        #6日
        xxx=df_all.groupby('ts_code')['chg_rank'].rolling(6).sum().reset_index()
        xxx.set_index(['level_1'], drop=True, append=False, inplace=True, verify_integrity=False)
        xxx.drop(['ts_code'],axis=1,inplace=True)

        df_all=df_all.join(xxx, lsuffix='', rsuffix='_6')

        df_all['chg_rank_6']=df_all.groupby('trade_date')['chg_rank_6'].rank(pct=True)
        df_all['chg_rank_6']=df_all['chg_rank_6']*10//2

        #10日
        xxx=df_all.groupby('ts_code')['chg_rank'].rolling(10).sum().reset_index()
        xxx.set_index(['level_1'], drop=True, append=False, inplace=True, verify_integrity=False)
        xxx.drop(['ts_code'],axis=1,inplace=True)

        df_all=df_all.join(xxx, lsuffix='', rsuffix='_10')

        df_all['chg_rank_10']=df_all.groupby('trade_date')['chg_rank_10'].rank(pct=True)
        df_all['chg_rank_10']=df_all['chg_rank_10']*10//2

        #3日
        xxx=df_all.groupby('ts_code')['chg_rank'].rolling(3).sum().reset_index()
        xxx.set_index(['level_1'], drop=True, append=False, inplace=True, verify_integrity=False)
        xxx.drop(['ts_code'],axis=1,inplace=True)

        df_all=df_all.join(xxx, lsuffix='', rsuffix='_3')

        df_all['chg_rank_3']=df_all.groupby('trade_date')['chg_rank_3'].rank(pct=True)
        df_all['chg_rank_3']=df_all['chg_rank_3']*10//2

        #print(df_all)

        #10日均量
        xxx=df_all.groupby('ts_code')['amount'].rolling(10).mean().reset_index()
        xxx.set_index(['level_1'], drop=True, append=False, inplace=True, verify_integrity=False)
        xxx.drop(['ts_code'],axis=1,inplace=True)
        df_all=df_all.join(xxx, lsuffix='', rsuffix='_10')

        #当日量占比
        df_all['pst_amount']=df_all['amount']/df_all['amount_10']
        df_all.drop(['amount_10'],axis=1,inplace=True)
        #当日量排名
        df_all['pst_amount_rank_10']=df_all.groupby('trade_date')['pst_amount'].rank(pct=True)
        df_all['pst_amount_rank_10']=df_all['pst_amount_rank_10']*10//2

        #计算三种比例rank
        dolist=['open','high','low']

        for curc in dolist:
            buffer=((df_all[curc]-df_all['pre_close'])*100)/df_all['pre_close']
            df_all[curc]=buffer
            df_all[curc]=df_all.groupby('trade_date')[curc].rank(pct=True)
            df_all[curc]=df_all[curc]*10//2

        #加入昨日rank
        df_all['yesterday_open']=df_all.groupby('ts_code')['open'].shift(1)
        df_all['yesterday_high']=df_all.groupby('ts_code')['high'].shift(1)
        df_all['yesterday_low']=df_all.groupby('ts_code')['low'].shift(1)
        df_all['yesterday_pst_amount_rank']=df_all.groupby('ts_code')['pst_amount_rank_10'].shift(1)
        ##加入前日open
        df_all['yesterday2_open']=df_all.groupby('ts_code')['open'].shift(2)
        df_all['yesterday2_high']=df_all.groupby('ts_code')['high'].shift(2)
        df_all['yesterday2_low']=df_all.groupby('ts_code')['low'].shift(2)
        df_all['yesterday2_pst_amount_rank']=df_all.groupby('ts_code')['pst_amount_rank_10'].shift(2)
        ##加入前日open
        df_all['yesterday3_open']=df_all.groupby('ts_code')['open'].shift(3)
        df_all['yesterday3_high']=df_all.groupby('ts_code')['high'].shift(3)
        df_all['yesterday3_low']=df_all.groupby('ts_code')['low'].shift(3)
        df_all['yesterday3_pst_amount_rank']=df_all.groupby('ts_code')['pst_amount_rank_10'].shift(3)

        df_all.drop(['close','pre_close','pct_chg','pst_amount','adj_factor','real_price','amount'],axis=1,inplace=True)



        #暂时不用的列
        df_all=df_all[df_all['high_stop']==0]
        #'tomorrow_chg'
        df_all.drop(['high_stop'],axis=1,inplace=True)



        df_all.dropna(axis=0,how='any',inplace=True)


        month_sec=df_all['trade_date'].max()
        df_all=df_all[df_all['trade_date']==month_sec]
        print(df_all)
        df_all=df_all.reset_index(drop=True)

        df_all.to_csv('today_train.csv')
        dwdw=1

class FEp30(FEbase):
    def __init__(self):
        pass

    def core(self,DataSetName):

        df_data=pd.read_csv(DataSetName[0],index_col=0,header=0)
        df_adj_all=pd.read_csv(DataSetName[1],index_col=0,header=0)
        #df_long_all=pd.read_csv(DataSetName[2],index_col=0,header=0)

        df_all=pd.merge(df_data, df_adj_all, how='inner', on=['ts_code','trade_date'])
        #df_all=pd.merge(df_all, df_long_all, how='inner', on=['ts_code','trade_date'])

        #===================================================================================================================================#
        #df_all['pe'] = df_all['pe'].fillna(9999)
        #df_all['pb'] = df_all['pb'].fillna(9999)

        #df_all['pe_rank']=df_all.groupby('trade_date')['pe'].rank(pct=True)
        #df_all['pb_rank']=df_all.groupby('trade_date')['pb'].rank(pct=True)        
        #df_all['pe_rank']=df_all['pe_rank']*10//1
        #df_all['pb_rank']=df_all['pb_rank']*10//1

        #df_all.drop(['turnover_rate','volume_ratio','pe','pb'],axis=1,inplace=True)

        #print(df_all)
        #df_all.to_csv('sjefosia.csv')

        #===================================================================================================================================#

        ##排除科创版
        #print(df_all)
        df_all=df_all[df_all['ts_code'].str.startswith('688')==False]
        

        ##加入gap_day特征
        #start=df_all['trade_date'].apply(str)[0]
        #end=df_all['trade_date'].apply(str)[df_all.shape[0]-1]
        #xxx=pd.date_range(start,end)

        #df = pd.DataFrame(xxx)
        #df.columns = ['trade_date']
        #df['trade_date']=df['trade_date'].map(str).map(lambda x : x[:4]+x[5:7]+x[8:10]).astype("int64")

        #yyy=df_all['trade_date']
        #zzz2=yyy.unique()
        #df_2=pd.DataFrame(zzz2)
        #df_2.columns = ['trade_date']
        #df_2['day_flag']=1
    
        #result = pd.merge(df, df_2, how='left', on=['trade_date'])
        #result['day_flag2']=result['day_flag'].shift(-1)
        #result['gap_day']=0

        #result.loc[(result['day_flag']==1) & (result['day_flag2']!=1),'gap_day']=1

        #result.drop(['day_flag','day_flag2'],axis=1,inplace=True)

        #df_all=pd.merge(df_all, result, how='left', on=['trade_date'])

        #===================================================================================================================================#

        #复权后价格
        df_all['real_price']=df_all['close']*df_all['adj_factor']
        
        #30日最低比值
        xxx=df_all.groupby('ts_code')['real_price'].rolling(30).min().reset_index()
        xxx.set_index(['level_1'], drop=True, append=False, inplace=True, verify_integrity=False)
        xxx.drop(['ts_code'],axis=1,inplace=True)
        
        df_all=df_all.join(xxx, lsuffix='', rsuffix='_30min')
        #bbb=df_all.groupby('ts_code')['real_price'].agg({'all_min':np.min, 'all_max': np.max}).reset_index()
        #ccc=pd.merge(df_all, bbb, how='inner', on=['ts_code'])
        df_all['30_pct']=(df_all['real_price']-df_all['real_price_30min'])/df_all['real_price_30min']
        df_all['30_pct_rank']=df_all.groupby('trade_date')['30_pct'].rank(pct=True)
        df_all['30_pct_rank']=df_all['30_pct_rank']*10//1

        #30日最高比值
        xxx=df_all.groupby('ts_code')['real_price'].rolling(30).max().reset_index()
        xxx.set_index(['level_1'], drop=True, append=False, inplace=True, verify_integrity=False)
        xxx.drop(['ts_code'],axis=1,inplace=True)
        
        df_all=df_all.join(xxx, lsuffix='', rsuffix='_30max')
        #bbb=df_all.groupby('ts_code')['real_price'].agg({'all_min':np.min, 'all_max': np.max}).reset_index()
        #ccc=pd.merge(df_all, bbb, how='inner', on=['ts_code'])
        df_all['30_pct_max']=(df_all['real_price']-df_all['real_price_30max'])/df_all['real_price_30max']
        df_all['30_pct_max_rank']=df_all.groupby('trade_date')['30_pct_max'].rank(pct=True)
        df_all['30_pct_max_rank']=df_all['30_pct_max_rank']*10//1



        df_all.drop(['30_pct','real_price_30min','30_pct_max','real_price_30max'],axis=1,inplace=True)
        #df_all.drop(['20_pct','real_price_20min','20_pct_max','real_price_20max'],axis=1,inplace=True)

        df_all.drop(['change','vol'],axis=1,inplace=True)

        #===================================================================================================================================#
    
        #明日幅度
        tm1=df_all.groupby('ts_code')['pct_chg'].shift(-1)
        tm2=df_all.groupby('ts_code')['pct_chg'].shift(-2)
        #df_all['tomorrow_chg']=((100+tm1)*(100+tm2)-10000)/100
        tm3=df_all.groupby('ts_code')['pct_chg'].shift(-3)
        tm4=df_all.groupby('ts_code')['pct_chg'].shift(-4)
        tm5=df_all.groupby('ts_code')['pct_chg'].shift(-5)

        df_all['tomorrow_chg']=(((100+tm1)/100)*((100+tm2)/100)*((100+tm3)/100)*((100+tm4)/100)*((100+tm5)/100)-1)*100

        #df_all['tomorrow_chg']=((100+tm1)*(100+tm2)*(100+tm3)-1000000)/10000
        #df_all['tomorrow_chg']=df_all.groupby('ts_code')['pct_chg'].shift(-1)
        #明日排名
        df_all['tomorrow_chg_rank']=df_all.groupby('trade_date')['tomorrow_chg'].rank(pct=True)
        df_all['tomorrow_chg_rank']=df_all['tomorrow_chg_rank']*9.9//1
        #是否停
        df_all['high_stop']=0
        df_all.loc[df_all['pct_chg']>9.4,'high_stop']=1
        df_all.loc[(df_all['pct_chg']<5.2) & (4.8<df_all['pct_chg']),'high_stop']=1


        ###真实价格范围(区分实际股价高低)
        #df_all['price_real_rank']=df_all.groupby('trade_date')['pre_close'].rank(pct=True)
        #df_all['price_real_rank']=df_all['price_real_rank']*10//1
        #1日
        df_all['chg_rank']=df_all.groupby('trade_date')['pct_chg'].rank(pct=True)
        df_all['chg_rank']=df_all['chg_rank']*10//1


        #6日
        xxx=df_all.groupby('ts_code')['chg_rank'].rolling(6).sum().reset_index()
        xxx.set_index(['level_1'], drop=True, append=False, inplace=True, verify_integrity=False)
        xxx.drop(['ts_code'],axis=1,inplace=True)

        df_all=df_all.join(xxx, lsuffix='', rsuffix='_6')

        df_all['chg_rank_6']=df_all.groupby('trade_date')['chg_rank_6'].rank(pct=True)
        df_all['chg_rank_6']=df_all['chg_rank_6']*10//1

        #10日
        xxx=df_all.groupby('ts_code')['chg_rank'].rolling(10).sum().reset_index()
        xxx.set_index(['level_1'], drop=True, append=False, inplace=True, verify_integrity=False)
        xxx.drop(['ts_code'],axis=1,inplace=True)

        df_all=df_all.join(xxx, lsuffix='', rsuffix='_10')

        df_all['chg_rank_10']=df_all.groupby('trade_date')['chg_rank_10'].rank(pct=True)
        df_all['chg_rank_10']=df_all['chg_rank_10']*10//1

        #3日
        xxx=df_all.groupby('ts_code')['chg_rank'].rolling(3).sum().reset_index()
        xxx.set_index(['level_1'], drop=True, append=False, inplace=True, verify_integrity=False)
        xxx.drop(['ts_code'],axis=1,inplace=True)

        df_all=df_all.join(xxx, lsuffix='', rsuffix='_3')

        df_all['chg_rank_3']=df_all.groupby('trade_date')['chg_rank_3'].rank(pct=True)
        df_all['chg_rank_3']=df_all['chg_rank_3']*10//1

        ##20日
        #xxx=df_all.groupby('ts_code')['chg_rank'].rolling(20).sum().reset_index()
        #xxx.set_index(['level_1'], drop=True, append=False, inplace=True, verify_integrity=False)
        #xxx.drop(['ts_code'],axis=1,inplace=True)

        #df_all=df_all.join(xxx, lsuffix='', rsuffix='_20')

        #df_all['chg_rank_20']=df_all.groupby('trade_date')['chg_rank_3'].rank(pct=True)
        #df_all['chg_rank_20']=df_all['chg_rank_20']*10//1

        #print(df_all)

        #10日均量
        xxx=df_all.groupby('ts_code')['amount'].rolling(10).mean().reset_index()
        xxx.set_index(['level_1'], drop=True, append=False, inplace=True, verify_integrity=False)
        xxx.drop(['ts_code'],axis=1,inplace=True)
        df_all=df_all.join(xxx, lsuffix='', rsuffix='_10')

        #当日量占比
        df_all['pst_amount']=df_all['amount']/df_all['amount_10']
        df_all.drop(['amount_10'],axis=1,inplace=True)
        #当日量排名
        df_all['pst_amount_rank_10']=df_all.groupby('trade_date')['pst_amount'].rank(pct=True)
        df_all['pst_amount_rank_10']=df_all['pst_amount_rank_10']*10//1

        ##5日均量
        #xxx=df_all.groupby('ts_code')['amount'].rolling(5).mean().reset_index()
        #xxx.set_index(['level_1'], drop=True, append=False, inplace=True, verify_integrity=False)
        #xxx.drop(['ts_code'],axis=1,inplace=True)
        #df_all=df_all.join(xxx, lsuffix='', rsuffix='_5')

        ##当日量占比
        #df_all['pst_amount']=df_all['amount']/df_all['amount_5']
        #df_all.drop(['amount_5'],axis=1,inplace=True)
        ##当日量排名
        #df_all['pst_amount_rank_5']=df_all.groupby('trade_date')['pst_amount'].rank(pct=True)
        #df_all['pst_amount_rank_5']=df_all['pst_amount_rank_5']*10//1

        #计算三种比例rank
        dolist=['open','high','low']

        for curc in dolist:
            buffer=((df_all[curc]-df_all['pre_close'])*100)/df_all['close']
            df_all[curc]=buffer
            df_all[curc]=df_all.groupby('trade_date')[curc].rank(pct=True)
            df_all[curc]=df_all[curc]*10//1

        #加入昨日rank
        df_all['yesterday_open']=df_all.groupby('ts_code')['open'].shift(1)
        df_all['yesterday_high']=df_all.groupby('ts_code')['high'].shift(1)
        df_all['yesterday_low']=df_all.groupby('ts_code')['low'].shift(1)
        df_all['yesterday_pst_amount_rank']=df_all.groupby('ts_code')['pst_amount_rank_10'].shift(1)
        ##加入前日open
        df_all['yesterday2_open']=df_all.groupby('ts_code')['open'].shift(2)
        df_all['yesterday2_high']=df_all.groupby('ts_code')['high'].shift(2)
        df_all['yesterday2_low']=df_all.groupby('ts_code')['low'].shift(2)
        df_all['yesterday2_pst_amount_rank']=df_all.groupby('ts_code')['pst_amount_rank_10'].shift(2)
        ##加入前日open
        df_all['yesterday3_open']=df_all.groupby('ts_code')['open'].shift(3)
        df_all['yesterday3_high']=df_all.groupby('ts_code')['high'].shift(3)
        df_all['yesterday3_low']=df_all.groupby('ts_code')['low'].shift(3)
        df_all['yesterday3_pst_amount_rank']=df_all.groupby('ts_code')['pst_amount_rank_10'].shift(3)


        df_all.drop(['close','pre_close','pct_chg','pst_amount','adj_factor','real_price','amount'],axis=1,inplace=True)
        #df_all.drop(['close','pre_close','pct_chg','adj_factor','real_price'],axis=1,inplace=True)

        #暂时不用的列
        df_all=df_all[df_all['high_stop']==0]

        #'tomorrow_chg'
        df_all.drop(['high_stop'],axis=1,inplace=True) 

        df_all.dropna(axis=0,how='any',inplace=True)

        print(df_all)
        df_all=df_all.reset_index(drop=True)

        return df_all

    def real_FE(self):

        df_data=pd.read_csv('real_now.csv',index_col=0,header=0)
        df_adj_all=pd.read_csv('real_adj_now.csv',index_col=0,header=0)

        df_all=pd.merge(df_data, df_adj_all, how='left', on=['ts_code','trade_date'])

        #df_all=df_all[df_all['ts_code'].str.startswith('688')==False]

        #df_all=pd.read_csv(bufferstring,index_col=0,header=0,nrows=100000)
    
        #df_all.drop(['change','vol'],axis=1,inplace=True)
 

        #===================================================================================================================================#
        
        #复权后价格
        df_all['adj_factor']=df_all['adj_factor'].fillna(0)
        df_all['real_price']=df_all['close']*df_all['adj_factor']
        
        df_all['real_price']=df_all.groupby('ts_code')['real_price'].shift(1)
        df_all['real_price']=df_all['real_price']*(1+df_all['pct_chg']/100)


        #30日最低比值
        xxx=df_all.groupby('ts_code')['real_price'].rolling(30).min().reset_index()
        xxx.set_index(['level_1'], drop=True, append=False, inplace=True, verify_integrity=False)
        xxx.drop(['ts_code'],axis=1,inplace=True)
        
        df_all=df_all.join(xxx, lsuffix='', rsuffix='_30min')
        #bbb=df_all.groupby('ts_code')['real_price'].agg({'all_min':np.min, 'all_max': np.max}).reset_index()
        #ccc=pd.merge(df_all, bbb, how='inner', on=['ts_code'])
        df_all['30_pct']=(df_all['real_price']-df_all['real_price_30min'])/df_all['real_price_30min']
        df_all['30_pct_rank']=df_all.groupby('trade_date')['30_pct'].rank(pct=True)
        df_all['30_pct_rank']=df_all['30_pct_rank']*10//1

        #30日最高比值
        xxx=df_all.groupby('ts_code')['real_price'].rolling(30).max().reset_index()
        xxx.set_index(['level_1'], drop=True, append=False, inplace=True, verify_integrity=False)
        xxx.drop(['ts_code'],axis=1,inplace=True)
        
        df_all=df_all.join(xxx, lsuffix='', rsuffix='_30max')
        #bbb=df_all.groupby('ts_code')['real_price'].agg({'all_min':np.min, 'all_max': np.max}).reset_index()
        #ccc=pd.merge(df_all, bbb, how='inner', on=['ts_code'])
        df_all['30_pct_max']=(df_all['real_price']-df_all['real_price_30max'])/df_all['real_price_30max']
        df_all['30_pct_max_rank']=df_all.groupby('trade_date')['30_pct_max'].rank(pct=True)
        df_all['30_pct_max_rank']=df_all['30_pct_max_rank']*10//1

        df_all.drop(['30_pct','real_price_30min','30_pct_max','real_price_30max'],axis=1,inplace=True)


        #===================================================================================================================================#


        #是否停
        df_all['high_stop']=0
        df_all.loc[df_all['pct_chg']>9.7,'high_stop']=1
        df_all.loc[(df_all['pct_chg']<5.2) & (4.8<df_all['pct_chg']),'high_stop']=1


        #1日
        df_all['chg_rank']=df_all.groupby('trade_date')['pct_chg'].rank(pct=True)
        df_all['chg_rank']=df_all['chg_rank']*10//1


        #6日
        xxx=df_all.groupby('ts_code')['chg_rank'].rolling(6).sum().reset_index()
        xxx.set_index(['level_1'], drop=True, append=False, inplace=True, verify_integrity=False)
        xxx.drop(['ts_code'],axis=1,inplace=True)

        df_all=df_all.join(xxx, lsuffix='', rsuffix='_6')

        df_all['chg_rank_6']=df_all.groupby('trade_date')['chg_rank_6'].rank(pct=True)
        df_all['chg_rank_6']=df_all['chg_rank_6']*10//1

        #10日
        xxx=df_all.groupby('ts_code')['chg_rank'].rolling(10).sum().reset_index()
        xxx.set_index(['level_1'], drop=True, append=False, inplace=True, verify_integrity=False)
        xxx.drop(['ts_code'],axis=1,inplace=True)

        df_all=df_all.join(xxx, lsuffix='', rsuffix='_10')

        df_all['chg_rank_10']=df_all.groupby('trade_date')['chg_rank_10'].rank(pct=True)
        df_all['chg_rank_10']=df_all['chg_rank_10']*10//1

        #3日
        xxx=df_all.groupby('ts_code')['chg_rank'].rolling(3).sum().reset_index()
        xxx.set_index(['level_1'], drop=True, append=False, inplace=True, verify_integrity=False)
        xxx.drop(['ts_code'],axis=1,inplace=True)

        df_all=df_all.join(xxx, lsuffix='', rsuffix='_3')

        df_all['chg_rank_3']=df_all.groupby('trade_date')['chg_rank_3'].rank(pct=True)
        df_all['chg_rank_3']=df_all['chg_rank_3']*10//1

        #print(df_all)

        #10日均量
        xxx=df_all.groupby('ts_code')['amount'].rolling(10).mean().reset_index()
        xxx.set_index(['level_1'], drop=True, append=False, inplace=True, verify_integrity=False)
        xxx.drop(['ts_code'],axis=1,inplace=True)
        df_all=df_all.join(xxx, lsuffix='', rsuffix='_10')

        #当日量占比
        df_all['pst_amount']=df_all['amount']/df_all['amount_10']
        df_all.drop(['amount_10'],axis=1,inplace=True)
        #当日量排名
        df_all['pst_amount_rank_10']=df_all.groupby('trade_date')['pst_amount'].rank(pct=True)
        df_all['pst_amount_rank_10']=df_all['pst_amount_rank_10']*10//1

        #计算三种比例rank
        dolist=['open','high','low']

        for curc in dolist:
            buffer=((df_all[curc]-df_all['pre_close'])*100)/df_all['pre_close']
            df_all[curc]=buffer
            df_all[curc]=df_all.groupby('trade_date')[curc].rank(pct=True)
            df_all[curc]=df_all[curc]*10//1

        #加入昨日rank
        df_all['yesterday_open']=df_all.groupby('ts_code')['open'].shift(1)
        df_all['yesterday_high']=df_all.groupby('ts_code')['high'].shift(1)
        df_all['yesterday_low']=df_all.groupby('ts_code')['low'].shift(1)
        df_all['yesterday_pst_amount_rank']=df_all.groupby('ts_code')['pst_amount_rank_10'].shift(1)
        ##加入前日open
        df_all['yesterday2_open']=df_all.groupby('ts_code')['open'].shift(2)
        df_all['yesterday2_high']=df_all.groupby('ts_code')['high'].shift(2)
        df_all['yesterday2_low']=df_all.groupby('ts_code')['low'].shift(2)
        df_all['yesterday2_pst_amount_rank']=df_all.groupby('ts_code')['pst_amount_rank_10'].shift(2)
        ##加入前日open
        df_all['yesterday3_open']=df_all.groupby('ts_code')['open'].shift(3)
        df_all['yesterday3_high']=df_all.groupby('ts_code')['high'].shift(3)
        df_all['yesterday3_low']=df_all.groupby('ts_code')['low'].shift(3)
        df_all['yesterday3_pst_amount_rank']=df_all.groupby('ts_code')['pst_amount_rank_10'].shift(3)

        df_all.drop(['close','pre_close','pct_chg','pst_amount','adj_factor','real_price','amount'],axis=1,inplace=True)



        #暂时不用的列
        df_all=df_all[df_all['high_stop']==0]
        #'tomorrow_chg'
        df_all.drop(['high_stop'],axis=1,inplace=True)



        df_all.dropna(axis=0,how='any',inplace=True)


        month_sec=df_all['trade_date'].max()
        df_all=df_all[df_all['trade_date']==month_sec]
        print(df_all)
        df_all=df_all.reset_index(drop=True)

        df_all.to_csv('today_train.csv')
        dwdw=1

class FEo30(FEbase):
    def __init__(self):
        pass

    def core(self,DataSetName):

        df_data=pd.read_csv(DataSetName[0],index_col=0,header=0)
        df_adj_all=pd.read_csv(DataSetName[1],index_col=0,header=0)
        #df_long_all=pd.read_csv(DataSetName[2],index_col=0,header=0)

        df_all=pd.merge(df_data, df_adj_all, how='inner', on=['ts_code','trade_date'])
        #df_all=pd.merge(df_all, df_long_all, how='inner', on=['ts_code','trade_date'])

        #===================================================================================================================================#
        #df_all['pe'] = df_all['pe'].fillna(9999)
        #df_all['pb'] = df_all['pb'].fillna(9999)

        #df_all['pe_rank']=df_all.groupby('trade_date')['pe'].rank(pct=True)
        #df_all['pb_rank']=df_all.groupby('trade_date')['pb'].rank(pct=True)        
        #df_all['pe_rank']=df_all['pe_rank']*10//1
        #df_all['pb_rank']=df_all['pb_rank']*10//1

        #df_all.drop(['turnover_rate','volume_ratio','pe','pb'],axis=1,inplace=True)

        #print(df_all)
        #df_all.to_csv('sjefosia.csv')

        #===================================================================================================================================#

        ##排除科创版
        #print(df_all)
        df_all=df_all[df_all['ts_code'].str.startswith('688')==False]
        

        ##加入gap_day特征
        #start=df_all['trade_date'].apply(str)[0]
        #end=df_all['trade_date'].apply(str)[df_all.shape[0]-1]
        #xxx=pd.date_range(start,end)

        #df = pd.DataFrame(xxx)
        #df.columns = ['trade_date']
        #df['trade_date']=df['trade_date'].map(str).map(lambda x : x[:4]+x[5:7]+x[8:10]).astype("int64")

        #yyy=df_all['trade_date']
        #zzz2=yyy.unique()
        #df_2=pd.DataFrame(zzz2)
        #df_2.columns = ['trade_date']
        #df_2['day_flag']=1
    
        #result = pd.merge(df, df_2, how='left', on=['trade_date'])
        #result['day_flag2']=result['day_flag'].shift(-1)
        #result['gap_day']=0

        #result.loc[(result['day_flag']==1) & (result['day_flag2']!=1),'gap_day']=1

        #result.drop(['day_flag','day_flag2'],axis=1,inplace=True)

        #df_all=pd.merge(df_all, result, how='left', on=['trade_date'])

        #===================================================================================================================================#

        #复权后价格
        df_all['real_price']=df_all['close']*df_all['adj_factor']
        
        #20日最低比值
        xxx=df_all.groupby('ts_code')['real_price'].rolling(20).min().reset_index()
        xxx.set_index(['level_1'], drop=True, append=False, inplace=True, verify_integrity=False)
        xxx.drop(['ts_code'],axis=1,inplace=True)
        
        df_all=df_all.join(xxx, lsuffix='', rsuffix='_20min')
        #bbb=df_all.groupby('ts_code')['real_price'].agg({'all_min':np.min, 'all_max': np.max}).reset_index()
        #ccc=pd.merge(df_all, bbb, how='inner', on=['ts_code'])
        df_all['20_pct']=(df_all['real_price']-df_all['real_price_20min'])/df_all['real_price_20min']
        df_all['20_pct_rank']=df_all.groupby('trade_date')['20_pct'].rank(pct=True)
        df_all['20_pct_rank']=df_all['20_pct_rank']*10//2

        #20日最高比值
        xxx=df_all.groupby('ts_code')['real_price'].rolling(20).max().reset_index()
        xxx.set_index(['level_1'], drop=True, append=False, inplace=True, verify_integrity=False)
        xxx.drop(['ts_code'],axis=1,inplace=True)
        
        df_all=df_all.join(xxx, lsuffix='', rsuffix='_20max')
        #bbb=df_all.groupby('ts_code')['real_price'].agg({'all_min':np.min, 'all_max': np.max}).reset_index()
        #ccc=pd.merge(df_all, bbb, how='inner', on=['ts_code'])
        df_all['20_pct_max']=(df_all['real_price']-df_all['real_price_20max'])/df_all['real_price_20max']
        df_all['20_pct_max_rank']=df_all.groupby('trade_date')['20_pct_max'].rank(pct=True)
        df_all['20_pct_max_rank']=df_all['20_pct_max_rank']*10//2

        df_all.drop(['20_pct','real_price_20min','20_pct_max','real_price_20max'],axis=1,inplace=True)
     
        #60日最低比值
        xxx=df_all.groupby('ts_code')['real_price'].rolling(60).min().reset_index()
        xxx.set_index(['level_1'], drop=True, append=False, inplace=True, verify_integrity=False)
        xxx.drop(['ts_code'],axis=1,inplace=True)
        
        df_all=df_all.join(xxx, lsuffix='', rsuffix='_60min')
        #bbb=df_all.groupby('ts_code')['real_price'].agg({'all_min':np.min, 'all_max': np.max}).reset_index()
        #ccc=pd.merge(df_all, bbb, how='inner', on=['ts_code'])
        df_all['60_pct']=(df_all['real_price']-df_all['real_price_60min'])/df_all['real_price_60min']
        df_all['60_pct_rank']=df_all.groupby('trade_date')['60_pct'].rank(pct=True)
        df_all['60_pct_rank']=df_all['60_pct_rank']*10//2

        #60日最高比值
        xxx=df_all.groupby('ts_code')['real_price'].rolling(60).max().reset_index()
        xxx.set_index(['level_1'], drop=True, append=False, inplace=True, verify_integrity=False)
        xxx.drop(['ts_code'],axis=1,inplace=True)
        
        df_all=df_all.join(xxx, lsuffix='', rsuffix='_60max')
        #bbb=df_all.groupby('ts_code')['real_price'].agg({'all_min':np.min, 'all_max': np.max}).reset_index()
        #ccc=pd.merge(df_all, bbb, how='inner', on=['ts_code'])
        df_all['60_pct_max']=(df_all['real_price']-df_all['real_price_60max'])/df_all['real_price_60max']
        df_all['60_pct_max_rank']=df_all.groupby('trade_date')['60_pct_max'].rank(pct=True)
        df_all['60_pct_max_rank']=df_all['60_pct_max_rank']*10//2



        df_all.drop(['60_pct','real_price_60min','60_pct_max','real_price_60max'],axis=1,inplace=True)
        #df_all.drop(['20_pct','real_price_20min','20_pct_max','real_price_20max'],axis=1,inplace=True)

        df_all.drop(['change','vol'],axis=1,inplace=True)

        #===================================================================================================================================#
    
        #明日幅度
        tm1=df_all.groupby('ts_code')['pct_chg'].shift(-1)
        tm2=df_all.groupby('ts_code')['pct_chg'].shift(-2)
        #df_all['tomorrow_chg']=((100+tm1)*(100+tm2)-10000)/100
        tm3=df_all.groupby('ts_code')['pct_chg'].shift(-3)
        tm4=df_all.groupby('ts_code')['pct_chg'].shift(-4)
        tm5=df_all.groupby('ts_code')['pct_chg'].shift(-5)

        df_all['tomorrow_chg']=(((100+tm1)/100)*((100+tm2)/100)*((100+tm3)/100)*((100+tm4)/100)*((100+tm5)/100)-1)*100

        #df_all['tomorrow_chg']=((100+tm1)*(100+tm2)*(100+tm3)-1000000)/10000
        #df_all['tomorrow_chg']=df_all.groupby('ts_code')['pct_chg'].shift(-1)
        #明日排名
        df_all['tomorrow_chg_rank']=df_all.groupby('trade_date')['tomorrow_chg'].rank(pct=True)
        df_all['tomorrow_chg_rank']=df_all['tomorrow_chg_rank']*9.9//1
        #是否停
        df_all['high_stop']=0
        df_all.loc[df_all['pct_chg']>9.4,'high_stop']=1
        df_all.loc[(df_all['pct_chg']<5.2) & (4.8<df_all['pct_chg']),'high_stop']=1


        ###真实价格范围(区分实际股价高低)
        #df_all['price_real_rank']=df_all.groupby('trade_date')['pre_close'].rank(pct=True)
        #df_all['price_real_rank']=df_all['price_real_rank']*10//1
        #1日
        df_all['chg_rank']=df_all.groupby('trade_date')['pct_chg'].rank(pct=True)
        df_all['chg_rank']=df_all['chg_rank']*10//2


        #6日
        xxx=df_all.groupby('ts_code')['chg_rank'].rolling(6).sum().reset_index()
        xxx.set_index(['level_1'], drop=True, append=False, inplace=True, verify_integrity=False)
        xxx.drop(['ts_code'],axis=1,inplace=True)

        df_all=df_all.join(xxx, lsuffix='', rsuffix='_6')

        df_all['chg_rank_6']=df_all.groupby('trade_date')['chg_rank_6'].rank(pct=True)
        df_all['chg_rank_6']=df_all['chg_rank_6']*10//2

        #10日
        xxx=df_all.groupby('ts_code')['chg_rank'].rolling(10).sum().reset_index()
        xxx.set_index(['level_1'], drop=True, append=False, inplace=True, verify_integrity=False)
        xxx.drop(['ts_code'],axis=1,inplace=True)

        df_all=df_all.join(xxx, lsuffix='', rsuffix='_10')

        df_all['chg_rank_10']=df_all.groupby('trade_date')['chg_rank_10'].rank(pct=True)
        df_all['chg_rank_10']=df_all['chg_rank_10']*10//2

        #3日
        xxx=df_all.groupby('ts_code')['chg_rank'].rolling(3).sum().reset_index()
        xxx.set_index(['level_1'], drop=True, append=False, inplace=True, verify_integrity=False)
        xxx.drop(['ts_code'],axis=1,inplace=True)

        df_all=df_all.join(xxx, lsuffix='', rsuffix='_3')

        df_all['chg_rank_3']=df_all.groupby('trade_date')['chg_rank_3'].rank(pct=True)
        df_all['chg_rank_3']=df_all['chg_rank_3']*10//2

        ##20日
        #xxx=df_all.groupby('ts_code')['chg_rank'].rolling(20).sum().reset_index()
        #xxx.set_index(['level_1'], drop=True, append=False, inplace=True, verify_integrity=False)
        #xxx.drop(['ts_code'],axis=1,inplace=True)

        #df_all=df_all.join(xxx, lsuffix='', rsuffix='_20')

        #df_all['chg_rank_20']=df_all.groupby('trade_date')['chg_rank_3'].rank(pct=True)
        #df_all['chg_rank_20']=df_all['chg_rank_20']*10//1

        #print(df_all)

        #10日均量
        xxx=df_all.groupby('ts_code')['amount'].rolling(10).mean().reset_index()
        xxx.set_index(['level_1'], drop=True, append=False, inplace=True, verify_integrity=False)
        xxx.drop(['ts_code'],axis=1,inplace=True)
        df_all=df_all.join(xxx, lsuffix='', rsuffix='_10')

        #当日量占比
        df_all['pst_amount']=df_all['amount']/df_all['amount_10']
        df_all.drop(['amount_10'],axis=1,inplace=True)
        #当日量排名
        df_all['pst_amount_rank_10']=df_all.groupby('trade_date')['pst_amount'].rank(pct=True)
        df_all['pst_amount_rank_10']=df_all['pst_amount_rank_10']*10//2

        ##5日均量
        #xxx=df_all.groupby('ts_code')['amount'].rolling(5).mean().reset_index()
        #xxx.set_index(['level_1'], drop=True, append=False, inplace=True, verify_integrity=False)
        #xxx.drop(['ts_code'],axis=1,inplace=True)
        #df_all=df_all.join(xxx, lsuffix='', rsuffix='_5')

        ##当日量占比
        #df_all['pst_amount']=df_all['amount']/df_all['amount_5']
        #df_all.drop(['amount_5'],axis=1,inplace=True)
        ##当日量排名
        #df_all['pst_amount_rank_5']=df_all.groupby('trade_date')['pst_amount'].rank(pct=True)
        #df_all['pst_amount_rank_5']=df_all['pst_amount_rank_5']*10//1

        #计算三种比例rank
        dolist=['open','high','low']

        for curc in dolist:
            buffer=((df_all[curc]-df_all['pre_close'])*100)/df_all['close']
            df_all[curc]=buffer
            df_all[curc]=df_all.groupby('trade_date')[curc].rank(pct=True)
            df_all[curc]=df_all[curc]*10//2

        #加入昨日rank
        df_all['yesterday_open']=df_all.groupby('ts_code')['open'].shift(1)
        df_all['yesterday_high']=df_all.groupby('ts_code')['high'].shift(1)
        df_all['yesterday_low']=df_all.groupby('ts_code')['low'].shift(1)
        df_all['yesterday_pst_amount_rank']=df_all.groupby('ts_code')['pst_amount_rank_10'].shift(1)
        ##加入前日open
        df_all['yesterday2_open']=df_all.groupby('ts_code')['open'].shift(2)
        df_all['yesterday2_high']=df_all.groupby('ts_code')['high'].shift(2)
        df_all['yesterday2_low']=df_all.groupby('ts_code')['low'].shift(2)
        df_all['yesterday2_pst_amount_rank']=df_all.groupby('ts_code')['pst_amount_rank_10'].shift(2)
        ##加入前日open
        df_all['yesterday3_open']=df_all.groupby('ts_code')['open'].shift(3)
        df_all['yesterday3_high']=df_all.groupby('ts_code')['high'].shift(3)
        df_all['yesterday3_low']=df_all.groupby('ts_code')['low'].shift(3)
        df_all['yesterday3_pst_amount_rank']=df_all.groupby('ts_code')['pst_amount_rank_10'].shift(3)


        df_all.drop(['close','pre_close','pct_chg','pst_amount','adj_factor','real_price','amount'],axis=1,inplace=True)
        #df_all.drop(['close','pre_close','pct_chg','adj_factor','real_price'],axis=1,inplace=True)

        #暂时不用的列
        df_all=df_all[df_all['high_stop']==0]

        #'tomorrow_chg'
        df_all.drop(['high_stop'],axis=1,inplace=True) 

        df_all.dropna(axis=0,how='any',inplace=True)

        print(df_all)
        df_all=df_all.reset_index(drop=True)

        return df_all

    def real_FE(self):

        df_data=pd.read_csv('real_now.csv',index_col=0,header=0)
        df_adj_all=pd.read_csv('real_adj_now.csv',index_col=0,header=0)

        df_all=pd.merge(df_data, df_adj_all, how='left', on=['ts_code','trade_date'])

        #df_all=df_all[df_all['ts_code'].str.startswith('688')==False]

        #df_all=pd.read_csv(bufferstring,index_col=0,header=0,nrows=100000)
    
        #df_all.drop(['change','vol'],axis=1,inplace=True)
 

        #===================================================================================================================================#
        
        #复权后价格
        df_all['adj_factor']=df_all['adj_factor'].fillna(0)
        df_all['real_price']=df_all['close']*df_all['adj_factor']
        
        df_all['real_price']=df_all.groupby('ts_code')['real_price'].shift(1)
        df_all['real_price']=df_all['real_price']*(1+df_all['pct_chg']/100)


        #30日最低比值
        xxx=df_all.groupby('ts_code')['real_price'].rolling(30).min().reset_index()
        xxx.set_index(['level_1'], drop=True, append=False, inplace=True, verify_integrity=False)
        xxx.drop(['ts_code'],axis=1,inplace=True)
        
        df_all=df_all.join(xxx, lsuffix='', rsuffix='_30min')
        #bbb=df_all.groupby('ts_code')['real_price'].agg({'all_min':np.min, 'all_max': np.max}).reset_index()
        #ccc=pd.merge(df_all, bbb, how='inner', on=['ts_code'])
        df_all['30_pct']=(df_all['real_price']-df_all['real_price_30min'])/df_all['real_price_30min']
        df_all['30_pct_rank']=df_all.groupby('trade_date')['30_pct'].rank(pct=True)
        df_all['30_pct_rank']=df_all['30_pct_rank']*10//2

        #30日最高比值
        xxx=df_all.groupby('ts_code')['real_price'].rolling(30).max().reset_index()
        xxx.set_index(['level_1'], drop=True, append=False, inplace=True, verify_integrity=False)
        xxx.drop(['ts_code'],axis=1,inplace=True)
        
        df_all=df_all.join(xxx, lsuffix='', rsuffix='_30max')
        #bbb=df_all.groupby('ts_code')['real_price'].agg({'all_min':np.min, 'all_max': np.max}).reset_index()
        #ccc=pd.merge(df_all, bbb, how='inner', on=['ts_code'])
        df_all['30_pct_max']=(df_all['real_price']-df_all['real_price_30max'])/df_all['real_price_30max']
        df_all['30_pct_max_rank']=df_all.groupby('trade_date')['30_pct_max'].rank(pct=True)
        df_all['30_pct_max_rank']=df_all['30_pct_max_rank']*10//2

        df_all.drop(['30_pct','real_price_30min','30_pct_max','real_price_30max'],axis=1,inplace=True)


        #===================================================================================================================================#


        #是否停
        df_all['high_stop']=0
        df_all.loc[df_all['pct_chg']>9.7,'high_stop']=1
        df_all.loc[(df_all['pct_chg']<5.2) & (4.8<df_all['pct_chg']),'high_stop']=1


        #1日
        df_all['chg_rank']=df_all.groupby('trade_date')['pct_chg'].rank(pct=True)
        df_all['chg_rank']=df_all['chg_rank']*10//2


        #6日
        xxx=df_all.groupby('ts_code')['chg_rank'].rolling(6).sum().reset_index()
        xxx.set_index(['level_1'], drop=True, append=False, inplace=True, verify_integrity=False)
        xxx.drop(['ts_code'],axis=1,inplace=True)

        df_all=df_all.join(xxx, lsuffix='', rsuffix='_6')

        df_all['chg_rank_6']=df_all.groupby('trade_date')['chg_rank_6'].rank(pct=True)
        df_all['chg_rank_6']=df_all['chg_rank_6']*10//2

        #10日
        xxx=df_all.groupby('ts_code')['chg_rank'].rolling(10).sum().reset_index()
        xxx.set_index(['level_1'], drop=True, append=False, inplace=True, verify_integrity=False)
        xxx.drop(['ts_code'],axis=1,inplace=True)

        df_all=df_all.join(xxx, lsuffix='', rsuffix='_10')

        df_all['chg_rank_10']=df_all.groupby('trade_date')['chg_rank_10'].rank(pct=True)
        df_all['chg_rank_10']=df_all['chg_rank_10']*10//2

        #3日
        xxx=df_all.groupby('ts_code')['chg_rank'].rolling(3).sum().reset_index()
        xxx.set_index(['level_1'], drop=True, append=False, inplace=True, verify_integrity=False)
        xxx.drop(['ts_code'],axis=1,inplace=True)

        df_all=df_all.join(xxx, lsuffix='', rsuffix='_3')

        df_all['chg_rank_3']=df_all.groupby('trade_date')['chg_rank_3'].rank(pct=True)
        df_all['chg_rank_3']=df_all['chg_rank_3']*10//2

        #print(df_all)

        #10日均量
        xxx=df_all.groupby('ts_code')['amount'].rolling(10).mean().reset_index()
        xxx.set_index(['level_1'], drop=True, append=False, inplace=True, verify_integrity=False)
        xxx.drop(['ts_code'],axis=1,inplace=True)
        df_all=df_all.join(xxx, lsuffix='', rsuffix='_10')

        #当日量占比
        df_all['pst_amount']=df_all['amount']/df_all['amount_10']
        df_all.drop(['amount_10'],axis=1,inplace=True)
        #当日量排名
        df_all['pst_amount_rank_10']=df_all.groupby('trade_date')['pst_amount'].rank(pct=True)
        df_all['pst_amount_rank_10']=df_all['pst_amount_rank_10']*10//2

        #计算三种比例rank
        dolist=['open','high','low']

        for curc in dolist:
            buffer=((df_all[curc]-df_all['pre_close'])*100)/df_all['pre_close']
            df_all[curc]=buffer
            df_all[curc]=df_all.groupby('trade_date')[curc].rank(pct=True)
            df_all[curc]=df_all[curc]*10//2

        #加入昨日rank
        df_all['yesterday_open']=df_all.groupby('ts_code')['open'].shift(1)
        df_all['yesterday_high']=df_all.groupby('ts_code')['high'].shift(1)
        df_all['yesterday_low']=df_all.groupby('ts_code')['low'].shift(1)
        df_all['yesterday_pst_amount_rank']=df_all.groupby('ts_code')['pst_amount_rank_10'].shift(1)
        ##加入前日open
        df_all['yesterday2_open']=df_all.groupby('ts_code')['open'].shift(2)
        df_all['yesterday2_high']=df_all.groupby('ts_code')['high'].shift(2)
        df_all['yesterday2_low']=df_all.groupby('ts_code')['low'].shift(2)
        df_all['yesterday2_pst_amount_rank']=df_all.groupby('ts_code')['pst_amount_rank_10'].shift(2)
        ##加入前日open
        df_all['yesterday3_open']=df_all.groupby('ts_code')['open'].shift(3)
        df_all['yesterday3_high']=df_all.groupby('ts_code')['high'].shift(3)
        df_all['yesterday3_low']=df_all.groupby('ts_code')['low'].shift(3)
        df_all['yesterday3_pst_amount_rank']=df_all.groupby('ts_code')['pst_amount_rank_10'].shift(3)

        df_all.drop(['close','pre_close','pct_chg','pst_amount','adj_factor','real_price','amount'],axis=1,inplace=True)



        #暂时不用的列
        df_all=df_all[df_all['high_stop']==0]
        #'tomorrow_chg'
        df_all.drop(['high_stop'],axis=1,inplace=True)



        df_all.dropna(axis=0,how='any',inplace=True)


        month_sec=df_all['trade_date'].max()
        df_all=df_all[df_all['trade_date']==month_sec]
        print(df_all)
        df_all=df_all.reset_index(drop=True)

        df_all.to_csv('today_train.csv')
        dwdw=1

class FEg30b(FEbase):
    def __init__(self):
        pass

    def core(self,DataSetName):

        df_data=pd.read_csv(DataSetName[0],index_col=0,header=0)
        df_adj_all=pd.read_csv(DataSetName[1],index_col=0,header=0)
        #df_long_all=pd.read_csv(DataSetName[2],index_col=0,header=0)

        df_all=pd.merge(df_data, df_adj_all, how='inner', on=['ts_code','trade_date'])
        #df_all=pd.merge(df_all, df_long_all, how='inner', on=['ts_code','trade_date'])

        #===================================================================================================================================#
        #df_all['pe'] = df_all['pe'].fillna(9999)
        #df_all['pb'] = df_all['pb'].fillna(9999)

        #df_all['pe_rank']=df_all.groupby('trade_date')['pe'].rank(pct=True)
        #df_all['pb_rank']=df_all.groupby('trade_date')['pb'].rank(pct=True)        
        #df_all['pe_rank']=df_all['pe_rank']*10//1
        #df_all['pb_rank']=df_all['pb_rank']*10//1

        #df_all.drop(['turnover_rate','volume_ratio','pe','pb'],axis=1,inplace=True)

        #print(df_all)
        #df_all.to_csv('sjefosia.csv')

        #===================================================================================================================================#

        ##排除科创版
        #print(df_all)
        df_all=df_all[df_all['ts_code'].str.startswith('688')==False]
        

        ##加入gap_day特征
        #start=df_all['trade_date'].apply(str)[0]
        #end=df_all['trade_date'].apply(str)[df_all.shape[0]-1]
        #xxx=pd.date_range(start,end)

        #df = pd.DataFrame(xxx)
        #df.columns = ['trade_date']
        #df['trade_date']=df['trade_date'].map(str).map(lambda x : x[:4]+x[5:7]+x[8:10]).astype("int64")

        #yyy=df_all['trade_date']
        #zzz2=yyy.unique()
        #df_2=pd.DataFrame(zzz2)
        #df_2.columns = ['trade_date']
        #df_2['day_flag']=1
    
        #result = pd.merge(df, df_2, how='left', on=['trade_date'])
        #result['day_flag2']=result['day_flag'].shift(-1)
        #result['gap_day']=0

        #result.loc[(result['day_flag']==1) & (result['day_flag2']!=1),'gap_day']=1

        #result.drop(['day_flag','day_flag2'],axis=1,inplace=True)

        #df_all=pd.merge(df_all, result, how='left', on=['trade_date'])

        #===================================================================================================================================#

        #复权后价格
        df_all['real_price']=df_all['close']*df_all['adj_factor']
        
        #30日最低比值
        xxx=df_all.groupby('ts_code')['real_price'].rolling(30).min().reset_index()
        xxx.set_index(['level_1'], drop=True, append=False, inplace=True, verify_integrity=False)
        xxx.drop(['ts_code'],axis=1,inplace=True)
        
        df_all=df_all.join(xxx, lsuffix='', rsuffix='_30min')
        #bbb=df_all.groupby('ts_code')['real_price'].agg({'all_min':np.min, 'all_max': np.max}).reset_index()
        #ccc=pd.merge(df_all, bbb, how='inner', on=['ts_code'])
        df_all['30_pct']=(df_all['real_price']-df_all['real_price_30min'])/df_all['real_price_30min']
        df_all['30_pct_rank']=df_all.groupby('trade_date')['30_pct'].rank(pct=True)
        df_all['30_pct_rank']=df_all['30_pct_rank']*10//2

        #30日最高比值
        xxx=df_all.groupby('ts_code')['real_price'].rolling(30).max().reset_index()
        xxx.set_index(['level_1'], drop=True, append=False, inplace=True, verify_integrity=False)
        xxx.drop(['ts_code'],axis=1,inplace=True)
        
        df_all=df_all.join(xxx, lsuffix='', rsuffix='_30max')
        #bbb=df_all.groupby('ts_code')['real_price'].agg({'all_min':np.min, 'all_max': np.max}).reset_index()
        #ccc=pd.merge(df_all, bbb, how='inner', on=['ts_code'])
        df_all['30_pct_max']=(df_all['real_price']-df_all['real_price_30max'])/df_all['real_price_30max']
        df_all['30_pct_max_rank']=df_all.groupby('trade_date')['30_pct_max'].rank(pct=True)
        df_all['30_pct_max_rank']=df_all['30_pct_max_rank']*10//2



        df_all.drop(['30_pct','real_price_30min','30_pct_max','real_price_30max'],axis=1,inplace=True)
        #df_all.drop(['20_pct','real_price_20min','20_pct_max','real_price_20max'],axis=1,inplace=True)

        df_all.drop(['change','vol'],axis=1,inplace=True)

        #===================================================================================================================================#
    
        #明日幅度
        tm1=df_all.groupby('ts_code')['pct_chg'].shift(-1)
        tm2=df_all.groupby('ts_code')['pct_chg'].shift(-2)
        #df_all['tomorrow_chg']=((100+tm1)*(100+tm2)-10000)/100
        tm3=df_all.groupby('ts_code')['pct_chg'].shift(-3)
        tm4=df_all.groupby('ts_code')['pct_chg'].shift(-4)
        tm5=df_all.groupby('ts_code')['pct_chg'].shift(-5)

        df_all['tomorrow_chg']=(((100+tm1)/100)*((100+tm2)/100)*((100+tm3)/100)*((100+tm4)/100)*((100+tm5)/100)-1)*100

        #df_all['tomorrow_chg']=((100+tm1)*(100+tm2)*(100+tm3)-1000000)/10000
        #df_all['tomorrow_chg']=df_all.groupby('ts_code')['pct_chg'].shift(-1)
        #明日排名
        df_all['tomorrow_chg_rank']=df_all.groupby('trade_date')['tomorrow_chg'].rank(pct=True)
        df_all['tomorrow_chg_rank']=df_all['tomorrow_chg_rank']*9.9//1
        #是否停
        df_all['high_stop']=0
        df_all.loc[df_all['pct_chg']>9.4,'high_stop']=1
        df_all.loc[(df_all['pct_chg']<5.2) & (4.8<df_all['pct_chg']),'high_stop']=1


        ###真实价格范围(区分实际股价高低)
        #df_all['price_real_rank']=df_all.groupby('trade_date')['pre_close'].rank(pct=True)
        #df_all['price_real_rank']=df_all['price_real_rank']*10//1
        #1日
        df_all['chg_rank']=df_all.groupby('trade_date')['pct_chg'].rank(pct=True)
        df_all['chg_rank']=df_all['chg_rank']*10//2


        #6日
        xxx=df_all.groupby('ts_code')['chg_rank'].rolling(6).sum().reset_index()
        xxx.set_index(['level_1'], drop=True, append=False, inplace=True, verify_integrity=False)
        xxx.drop(['ts_code'],axis=1,inplace=True)

        df_all=df_all.join(xxx, lsuffix='', rsuffix='_6')

        df_all['chg_rank_6']=df_all.groupby('trade_date')['chg_rank_6'].rank(pct=True)
        df_all['chg_rank_6']=df_all['chg_rank_6']*10//2

        #10日
        xxx=df_all.groupby('ts_code')['chg_rank'].rolling(10).sum().reset_index()
        xxx.set_index(['level_1'], drop=True, append=False, inplace=True, verify_integrity=False)
        xxx.drop(['ts_code'],axis=1,inplace=True)

        df_all=df_all.join(xxx, lsuffix='', rsuffix='_10')

        df_all['chg_rank_10']=df_all.groupby('trade_date')['chg_rank_10'].rank(pct=True)
        df_all['chg_rank_10']=df_all['chg_rank_10']*10//2

        #3日
        xxx=df_all.groupby('ts_code')['chg_rank'].rolling(3).sum().reset_index()
        xxx.set_index(['level_1'], drop=True, append=False, inplace=True, verify_integrity=False)
        xxx.drop(['ts_code'],axis=1,inplace=True)

        df_all=df_all.join(xxx, lsuffix='', rsuffix='_3')

        df_all['chg_rank_3']=df_all.groupby('trade_date')['chg_rank_3'].rank(pct=True)
        df_all['chg_rank_3']=df_all['chg_rank_3']*10//2

        ##20日
        #xxx=df_all.groupby('ts_code')['chg_rank'].rolling(20).sum().reset_index()
        #xxx.set_index(['level_1'], drop=True, append=False, inplace=True, verify_integrity=False)
        #xxx.drop(['ts_code'],axis=1,inplace=True)

        #df_all=df_all.join(xxx, lsuffix='', rsuffix='_20')

        #df_all['chg_rank_20']=df_all.groupby('trade_date')['chg_rank_3'].rank(pct=True)
        #df_all['chg_rank_20']=df_all['chg_rank_20']*10//1

        #print(df_all)

        #10日均量
        xxx=df_all.groupby('ts_code')['amount'].rolling(10).mean().reset_index()
        xxx.set_index(['level_1'], drop=True, append=False, inplace=True, verify_integrity=False)
        xxx.drop(['ts_code'],axis=1,inplace=True)
        df_all=df_all.join(xxx, lsuffix='', rsuffix='_10')

        #当日量占比
        df_all['pst_amount']=df_all['amount']/df_all['amount_10']
        df_all.drop(['amount_10'],axis=1,inplace=True)
        #当日量排名
        df_all['pst_amount_rank_10']=df_all.groupby('trade_date')['pst_amount'].rank(pct=True)
        df_all['pst_amount_rank_10']=df_all['pst_amount_rank_10']*10//2

        ##5日均量
        #xxx=df_all.groupby('ts_code')['amount'].rolling(5).mean().reset_index()
        #xxx.set_index(['level_1'], drop=True, append=False, inplace=True, verify_integrity=False)
        #xxx.drop(['ts_code'],axis=1,inplace=True)
        #df_all=df_all.join(xxx, lsuffix='', rsuffix='_5')

        ##当日量占比
        #df_all['pst_amount']=df_all['amount']/df_all['amount_5']
        #df_all.drop(['amount_5'],axis=1,inplace=True)
        ##当日量排名
        #df_all['pst_amount_rank_5']=df_all.groupby('trade_date')['pst_amount'].rank(pct=True)
        #df_all['pst_amount_rank_5']=df_all['pst_amount_rank_5']*10//1

        #计算三种比例rank
        dolist=['open','high','low']

        for curc in dolist:
            buffer=((df_all[curc]-df_all['pre_close'])*100)/df_all['close']
            df_all[curc]=buffer
            df_all[curc]=df_all.groupby('trade_date')[curc].rank(pct=True)
            df_all[curc]=df_all[curc]*10//2

        #加入昨日rank
        df_all['yesterday_open']=df_all.groupby('ts_code')['open'].shift(1)
        df_all['yesterday_high']=df_all.groupby('ts_code')['high'].shift(1)
        df_all['yesterday_low']=df_all.groupby('ts_code')['low'].shift(1)
        df_all['yesterday_pst_amount_rank']=df_all.groupby('ts_code')['pst_amount_rank_10'].shift(1)
        ##加入前日open
        df_all['yesterday2_open']=df_all.groupby('ts_code')['open'].shift(2)
        df_all['yesterday2_high']=df_all.groupby('ts_code')['high'].shift(2)
        df_all['yesterday2_low']=df_all.groupby('ts_code')['low'].shift(2)
        df_all['yesterday2_pst_amount_rank']=df_all.groupby('ts_code')['pst_amount_rank_10'].shift(2)
        ##加入前日open
        df_all['yesterday3_open']=df_all.groupby('ts_code')['open'].shift(3)
        df_all['yesterday3_high']=df_all.groupby('ts_code')['high'].shift(3)
        df_all['yesterday3_low']=df_all.groupby('ts_code')['low'].shift(3)
        df_all['yesterday3_pst_amount_rank']=df_all.groupby('ts_code')['pst_amount_rank_10'].shift(3)
        ##加入前日open
        df_all['yesterday4_open']=df_all.groupby('ts_code')['open'].shift(4)
        df_all['yesterday4_high']=df_all.groupby('ts_code')['high'].shift(4)
        df_all['yesterday4_low']=df_all.groupby('ts_code')['low'].shift(4)
        df_all['yesterday4_pst_amount_rank']=df_all.groupby('ts_code')['pst_amount_rank_10'].shift(4)
        ##加入前日open
        df_all['yesterday5_open']=df_all.groupby('ts_code')['open'].shift(5)
        df_all['yesterday5_high']=df_all.groupby('ts_code')['high'].shift(5)
        df_all['yesterday5_low']=df_all.groupby('ts_code')['low'].shift(5)
        df_all['yesterday5_pst_amount_rank']=df_all.groupby('ts_code')['pst_amount_rank_10'].shift(5)

        df_all.drop(['close','pre_close','pct_chg','pst_amount','adj_factor','real_price','amount'],axis=1,inplace=True)
        #df_all.drop(['close','pre_close','pct_chg','adj_factor','real_price'],axis=1,inplace=True)

        #暂时不用的列
        df_all=df_all[df_all['high_stop']==0]

        #'tomorrow_chg'
        df_all.drop(['high_stop'],axis=1,inplace=True) 

        df_all.dropna(axis=0,how='any',inplace=True)

        print(df_all)
        df_all=df_all.reset_index(drop=True)

        return df_all

    def real_FE(self):

        df_data=pd.read_csv('real_now.csv',index_col=0,header=0)
        df_adj_all=pd.read_csv('real_adj_now.csv',index_col=0,header=0)

        df_all=pd.merge(df_data, df_adj_all, how='left', on=['ts_code','trade_date'])

        #df_all=df_all[df_all['ts_code'].str.startswith('688')==False]

        #df_all=pd.read_csv(bufferstring,index_col=0,header=0,nrows=100000)
    
        #df_all.drop(['change','vol'],axis=1,inplace=True)
 

        #===================================================================================================================================#
        
        #复权后价格
        df_all['adj_factor']=df_all['adj_factor'].fillna(0)
        df_all['real_price']=df_all['close']*df_all['adj_factor']
        
        df_all['real_price']=df_all.groupby('ts_code')['real_price'].shift(1)
        df_all['real_price']=df_all['real_price']*(1+df_all['pct_chg']/100)


        #30日最低比值
        xxx=df_all.groupby('ts_code')['real_price'].rolling(30).min().reset_index()
        xxx.set_index(['level_1'], drop=True, append=False, inplace=True, verify_integrity=False)
        xxx.drop(['ts_code'],axis=1,inplace=True)
        
        df_all=df_all.join(xxx, lsuffix='', rsuffix='_30min')
        #bbb=df_all.groupby('ts_code')['real_price'].agg({'all_min':np.min, 'all_max': np.max}).reset_index()
        #ccc=pd.merge(df_all, bbb, how='inner', on=['ts_code'])
        df_all['30_pct']=(df_all['real_price']-df_all['real_price_30min'])/df_all['real_price_30min']
        df_all['30_pct_rank']=df_all.groupby('trade_date')['30_pct'].rank(pct=True)
        df_all['30_pct_rank']=df_all['30_pct_rank']*10//2

        #30日最高比值
        xxx=df_all.groupby('ts_code')['real_price'].rolling(30).max().reset_index()
        xxx.set_index(['level_1'], drop=True, append=False, inplace=True, verify_integrity=False)
        xxx.drop(['ts_code'],axis=1,inplace=True)
        
        df_all=df_all.join(xxx, lsuffix='', rsuffix='_30max')
        #bbb=df_all.groupby('ts_code')['real_price'].agg({'all_min':np.min, 'all_max': np.max}).reset_index()
        #ccc=pd.merge(df_all, bbb, how='inner', on=['ts_code'])
        df_all['30_pct_max']=(df_all['real_price']-df_all['real_price_30max'])/df_all['real_price_30max']
        df_all['30_pct_max_rank']=df_all.groupby('trade_date')['30_pct_max'].rank(pct=True)
        df_all['30_pct_max_rank']=df_all['30_pct_max_rank']*10//2

        df_all.drop(['30_pct','real_price_30min','30_pct_max','real_price_30max'],axis=1,inplace=True)


        #===================================================================================================================================#


        #是否停
        df_all['high_stop']=0
        df_all.loc[df_all['pct_chg']>9.7,'high_stop']=1
        df_all.loc[(df_all['pct_chg']<5.2) & (4.8<df_all['pct_chg']),'high_stop']=1


        #1日
        df_all['chg_rank']=df_all.groupby('trade_date')['pct_chg'].rank(pct=True)
        df_all['chg_rank']=df_all['chg_rank']*10//2


        #6日
        xxx=df_all.groupby('ts_code')['chg_rank'].rolling(6).sum().reset_index()
        xxx.set_index(['level_1'], drop=True, append=False, inplace=True, verify_integrity=False)
        xxx.drop(['ts_code'],axis=1,inplace=True)

        df_all=df_all.join(xxx, lsuffix='', rsuffix='_6')

        df_all['chg_rank_6']=df_all.groupby('trade_date')['chg_rank_6'].rank(pct=True)
        df_all['chg_rank_6']=df_all['chg_rank_6']*10//2

        #10日
        xxx=df_all.groupby('ts_code')['chg_rank'].rolling(10).sum().reset_index()
        xxx.set_index(['level_1'], drop=True, append=False, inplace=True, verify_integrity=False)
        xxx.drop(['ts_code'],axis=1,inplace=True)

        df_all=df_all.join(xxx, lsuffix='', rsuffix='_10')

        df_all['chg_rank_10']=df_all.groupby('trade_date')['chg_rank_10'].rank(pct=True)
        df_all['chg_rank_10']=df_all['chg_rank_10']*10//2

        #3日
        xxx=df_all.groupby('ts_code')['chg_rank'].rolling(3).sum().reset_index()
        xxx.set_index(['level_1'], drop=True, append=False, inplace=True, verify_integrity=False)
        xxx.drop(['ts_code'],axis=1,inplace=True)

        df_all=df_all.join(xxx, lsuffix='', rsuffix='_3')

        df_all['chg_rank_3']=df_all.groupby('trade_date')['chg_rank_3'].rank(pct=True)
        df_all['chg_rank_3']=df_all['chg_rank_3']*10//2

        #print(df_all)

        #10日均量
        xxx=df_all.groupby('ts_code')['amount'].rolling(10).mean().reset_index()
        xxx.set_index(['level_1'], drop=True, append=False, inplace=True, verify_integrity=False)
        xxx.drop(['ts_code'],axis=1,inplace=True)
        df_all=df_all.join(xxx, lsuffix='', rsuffix='_10')

        #当日量占比
        df_all['pst_amount']=df_all['amount']/df_all['amount_10']
        df_all.drop(['amount_10'],axis=1,inplace=True)
        #当日量排名
        df_all['pst_amount_rank_10']=df_all.groupby('trade_date')['pst_amount'].rank(pct=True)
        df_all['pst_amount_rank_10']=df_all['pst_amount_rank_10']*10//2

        #计算三种比例rank
        dolist=['open','high','low']

        for curc in dolist:
            buffer=((df_all[curc]-df_all['pre_close'])*100)/df_all['pre_close']
            df_all[curc]=buffer
            df_all[curc]=df_all.groupby('trade_date')[curc].rank(pct=True)
            df_all[curc]=df_all[curc]*10//2

        #加入昨日rank
        df_all['yesterday_open']=df_all.groupby('ts_code')['open'].shift(1)
        df_all['yesterday_high']=df_all.groupby('ts_code')['high'].shift(1)
        df_all['yesterday_low']=df_all.groupby('ts_code')['low'].shift(1)
        df_all['yesterday_pst_amount_rank']=df_all.groupby('ts_code')['pst_amount_rank_10'].shift(1)
        ##加入前日open
        df_all['yesterday2_open']=df_all.groupby('ts_code')['open'].shift(2)
        df_all['yesterday2_high']=df_all.groupby('ts_code')['high'].shift(2)
        df_all['yesterday2_low']=df_all.groupby('ts_code')['low'].shift(2)
        df_all['yesterday2_pst_amount_rank']=df_all.groupby('ts_code')['pst_amount_rank_10'].shift(2)
        ##加入前日open
        df_all['yesterday3_open']=df_all.groupby('ts_code')['open'].shift(3)
        df_all['yesterday3_high']=df_all.groupby('ts_code')['high'].shift(3)
        df_all['yesterday3_low']=df_all.groupby('ts_code')['low'].shift(3)
        df_all['yesterday3_pst_amount_rank']=df_all.groupby('ts_code')['pst_amount_rank_10'].shift(3)

        df_all.drop(['close','pre_close','pct_chg','pst_amount','adj_factor','real_price','amount'],axis=1,inplace=True)



        #暂时不用的列
        df_all=df_all[df_all['high_stop']==0]
        #'tomorrow_chg'
        df_all.drop(['high_stop'],axis=1,inplace=True)



        df_all.dropna(axis=0,how='any',inplace=True)


        month_sec=df_all['trade_date'].max()
        df_all=df_all[df_all['trade_date']==month_sec]
        print(df_all)
        df_all=df_all.reset_index(drop=True)

        df_all.to_csv('today_train.csv')
        dwdw=1

class FEl30(FEbase):
    def __init__(self):
        pass

    def core(self,DataSetName):

        df_data=pd.read_csv(DataSetName[0],index_col=0,header=0)
        df_adj_all=pd.read_csv(DataSetName[1],index_col=0,header=0)
        #df_long_all=pd.read_csv(DataSetName[2],index_col=0,header=0)

        df_all=pd.merge(df_data, df_adj_all, how='inner', on=['ts_code','trade_date'])
        #df_all=pd.merge(df_all, df_long_all, how='inner', on=['ts_code','trade_date'])

        #===================================================================================================================================#
        #df_all['pe'] = df_all['pe'].fillna(9999)
        #df_all['pb'] = df_all['pb'].fillna(9999)

        #df_all['pe_rank']=df_all.groupby('trade_date')['pe'].rank(pct=True)
        #df_all['pb_rank']=df_all.groupby('trade_date')['pb'].rank(pct=True)        
        #df_all['pe_rank']=df_all['pe_rank']*10//1
        #df_all['pb_rank']=df_all['pb_rank']*10//1

        #df_all.drop(['turnover_rate','volume_ratio','pe','pb'],axis=1,inplace=True)

        #print(df_all)
        #df_all.to_csv('sjefosia.csv')

        #===================================================================================================================================#

        ##排除科创版
        #print(df_all)
        df_all=df_all[df_all['ts_code'].str.startswith('688')==False]
        

        ##加入gap_day特征
        #start=df_all['trade_date'].apply(str)[0]
        #end=df_all['trade_date'].apply(str)[df_all.shape[0]-1]
        #xxx=pd.date_range(start,end)

        #df = pd.DataFrame(xxx)
        #df.columns = ['trade_date']
        #df['trade_date']=df['trade_date'].map(str).map(lambda x : x[:4]+x[5:7]+x[8:10]).astype("int64")

        #yyy=df_all['trade_date']
        #zzz2=yyy.unique()
        #df_2=pd.DataFrame(zzz2)
        #df_2.columns = ['trade_date']
        #df_2['day_flag']=1
    
        #result = pd.merge(df, df_2, how='left', on=['trade_date'])
        #result['day_flag2']=result['day_flag'].shift(-1)
        #result['gap_day']=0

        #result.loc[(result['day_flag']==1) & (result['day_flag2']!=1),'gap_day']=1

        #result.drop(['day_flag','day_flag2'],axis=1,inplace=True)

        #df_all=pd.merge(df_all, result, how='left', on=['trade_date'])

        #===================================================================================================================================#

        #复权后价格
        df_all['real_price']=df_all['close']*df_all['adj_factor']
        
        #30日最低比值
        xxx=df_all.groupby('ts_code')['real_price'].rolling(30).min().reset_index()
        xxx.set_index(['level_1'], drop=True, append=False, inplace=True, verify_integrity=False)
        xxx.drop(['ts_code'],axis=1,inplace=True)
        
        df_all=df_all.join(xxx, lsuffix='', rsuffix='_30min')
        #bbb=df_all.groupby('ts_code')['real_price'].agg({'all_min':np.min, 'all_max': np.max}).reset_index()
        #ccc=pd.merge(df_all, bbb, how='inner', on=['ts_code'])
        df_all['30_pct']=(df_all['real_price']-df_all['real_price_30min'])/df_all['real_price_30min']
        df_all['30_pct_rank']=df_all.groupby('trade_date')['30_pct'].rank(pct=True)
        df_all['30_pct_rank']=df_all['30_pct_rank']*10//2

        #30日最高比值
        xxx=df_all.groupby('ts_code')['real_price'].rolling(30).max().reset_index()
        xxx.set_index(['level_1'], drop=True, append=False, inplace=True, verify_integrity=False)
        xxx.drop(['ts_code'],axis=1,inplace=True)
        
        df_all=df_all.join(xxx, lsuffix='', rsuffix='_30max')
        #bbb=df_all.groupby('ts_code')['real_price'].agg({'all_min':np.min, 'all_max': np.max}).reset_index()
        #ccc=pd.merge(df_all, bbb, how='inner', on=['ts_code'])
        df_all['30_pct_max']=(df_all['real_price']-df_all['real_price_30max'])/df_all['real_price_30max']
        df_all['30_pct_max_rank']=df_all.groupby('trade_date')['30_pct_max'].rank(pct=True)
        df_all['30_pct_max_rank']=df_all['30_pct_max_rank']*10//2



        df_all.drop(['30_pct','real_price_30min','30_pct_max','real_price_30max'],axis=1,inplace=True)
        #df_all.drop(['20_pct','real_price_20min','20_pct_max','real_price_20max'],axis=1,inplace=True)

        df_all.drop(['change','vol'],axis=1,inplace=True)

        #===================================================================================================================================#
    
        #明日幅度
        tm1=df_all.groupby('ts_code')['pct_chg'].shift(-1)
        tm2=df_all.groupby('ts_code')['pct_chg'].shift(-2)
        #df_all['tomorrow_chg']=((100+tm1)*(100+tm2)-10000)/100
        tm3=df_all.groupby('ts_code')['pct_chg'].shift(-3)
        tm4=df_all.groupby('ts_code')['pct_chg'].shift(-4)
        tm5=df_all.groupby('ts_code')['pct_chg'].shift(-5)
        #tm6=df_all.groupby('ts_code')['pct_chg'].shift(-6)
        #tm7=df_all.groupby('ts_code')['pct_chg'].shift(-7)
        ##df_all['tomorrow_chg']=((100+tm1)*(100+tm2)-10000)/100
        #tm8=df_all.groupby('ts_code')['pct_chg'].shift(-8)
        #tm9=df_all.groupby('ts_code')['pct_chg'].shift(-9)
        #tm10=df_all.groupby('ts_code')['pct_chg'].shift(-10)
        #tm11=df_all.groupby('ts_code')['pct_chg'].shift(-11)
        #tm12=df_all.groupby('ts_code')['pct_chg'].shift(-12)
        ##df_all['tomorrow_chg']=((100+tm1)*(100+tm2)-10000)/100
        #tm13=df_all.groupby('ts_code')['pct_chg'].shift(-13)
        #tm14=df_all.groupby('ts_code')['pct_chg'].shift(-14)
        #tm15=df_all.groupby('ts_code')['pct_chg'].shift(-15)
        #tm16=df_all.groupby('ts_code')['pct_chg'].shift(-16)
        #tm17=df_all.groupby('ts_code')['pct_chg'].shift(-17)
        ##df_all['tomorrow_chg']=((100+tm1)*(100+tm2)-10000)/100
        #tm18=df_all.groupby('ts_code')['pct_chg'].shift(-18)
        #tm19=df_all.groupby('ts_code')['pct_chg'].shift(-19)
        #tm20=df_all.groupby('ts_code')['pct_chg'].shift(-20)

        #df_all['tomorrow_chg']=(((100+tm1)/100)*((100+tm2)/100)*((100+tm3)/100)*((100+tm4)/100)*((100+tm5)/100)*((100+tm6)/100)*((100+tm7)/100)*((100+tm8)/100)*((100+tm9)/100)*((100+tm10)/100)*((100+tm11)/100)*((100+tm12)/100)*((100+tm13)/100)*((100+tm14)/100)*((100+tm15)/100)*((100+tm16)/100)*((100+tm17)/100)*((100+tm18)/100)*((100+tm19)/100)*((100+tm20)/100)-1)*100
        df_all['tomorrow_chg']=(((100+tm1)/100)*((100+tm2)/100)*((100+tm3)/100)*((100+tm4)/100)*((100+tm5)/100)-1)*100

        #df_all['tomorrow_chg']=((100+tm1)*(100+tm2)*(100+tm3)-1000000)/10000
        #df_all['tomorrow_chg']=df_all.groupby('ts_code')['pct_chg'].shift(-1)
        #明日排名
        df_all['tomorrow_chg_rank']=df_all.groupby('trade_date')['tomorrow_chg'].rank(pct=True)
        df_all['tomorrow_chg_rank']=df_all['tomorrow_chg_rank']*9.9//1
        #是否停
        df_all['high_stop']=0
        df_all.loc[df_all['pct_chg']>9.4,'high_stop']=1
        df_all.loc[(df_all['pct_chg']<5.2) & (4.8<df_all['pct_chg']),'high_stop']=1


        ###真实价格范围(区分实际股价高低)
        #df_all['price_real_rank']=df_all.groupby('trade_date')['pre_close'].rank(pct=True)
        #df_all['price_real_rank']=df_all['price_real_rank']*10//1
        #1日
        df_all['chg_rank']=df_all.groupby('trade_date')['pct_chg'].rank(pct=True)
        df_all['chg_rank']=df_all['chg_rank']*10//2


        #6日
        xxx=df_all.groupby('ts_code')['chg_rank'].rolling(6).sum().reset_index()
        xxx.set_index(['level_1'], drop=True, append=False, inplace=True, verify_integrity=False)
        xxx.drop(['ts_code'],axis=1,inplace=True)

        df_all=df_all.join(xxx, lsuffix='', rsuffix='_6')

        df_all['chg_rank_6']=df_all.groupby('trade_date')['chg_rank_6'].rank(pct=True)
        df_all['chg_rank_6']=df_all['chg_rank_6']*10//2

        #10日
        xxx=df_all.groupby('ts_code')['chg_rank'].rolling(10).sum().reset_index()
        xxx.set_index(['level_1'], drop=True, append=False, inplace=True, verify_integrity=False)
        xxx.drop(['ts_code'],axis=1,inplace=True)

        df_all=df_all.join(xxx, lsuffix='', rsuffix='_10')

        df_all['chg_rank_10']=df_all.groupby('trade_date')['chg_rank_10'].rank(pct=True)
        df_all['chg_rank_10']=df_all['chg_rank_10']*10//2

        #3日
        xxx=df_all.groupby('ts_code')['chg_rank'].rolling(3).sum().reset_index()
        xxx.set_index(['level_1'], drop=True, append=False, inplace=True, verify_integrity=False)
        xxx.drop(['ts_code'],axis=1,inplace=True)

        df_all=df_all.join(xxx, lsuffix='', rsuffix='_3')

        df_all['chg_rank_3']=df_all.groupby('trade_date')['chg_rank_3'].rank(pct=True)
        df_all['chg_rank_3']=df_all['chg_rank_3']*10//2

        ##20日
        #xxx=df_all.groupby('ts_code')['chg_rank'].rolling(20).sum().reset_index()
        #xxx.set_index(['level_1'], drop=True, append=False, inplace=True, verify_integrity=False)
        #xxx.drop(['ts_code'],axis=1,inplace=True)

        #df_all=df_all.join(xxx, lsuffix='', rsuffix='_20')

        #df_all['chg_rank_20']=df_all.groupby('trade_date')['chg_rank_3'].rank(pct=True)
        #df_all['chg_rank_20']=df_all['chg_rank_20']*10//1

        #print(df_all)

        #10日均量
        xxx=df_all.groupby('ts_code')['amount'].rolling(10).mean().reset_index()
        xxx.set_index(['level_1'], drop=True, append=False, inplace=True, verify_integrity=False)
        xxx.drop(['ts_code'],axis=1,inplace=True)
        df_all=df_all.join(xxx, lsuffix='', rsuffix='_10')

        #当日量占比
        df_all['pst_amount']=df_all['amount']/df_all['amount_10']
        df_all.drop(['amount_10'],axis=1,inplace=True)
        #当日量排名
        df_all['pst_amount_rank_10']=df_all.groupby('trade_date')['pst_amount'].rank(pct=True)
        df_all['pst_amount_rank_10']=df_all['pst_amount_rank_10']*10//2

        ##5日均量
        #xxx=df_all.groupby('ts_code')['amount'].rolling(5).mean().reset_index()
        #xxx.set_index(['level_1'], drop=True, append=False, inplace=True, verify_integrity=False)
        #xxx.drop(['ts_code'],axis=1,inplace=True)
        #df_all=df_all.join(xxx, lsuffix='', rsuffix='_5')

        ##当日量占比
        #df_all['pst_amount']=df_all['amount']/df_all['amount_5']
        #df_all.drop(['amount_5'],axis=1,inplace=True)
        ##当日量排名
        #df_all['pst_amount_rank_5']=df_all.groupby('trade_date')['pst_amount'].rank(pct=True)
        #df_all['pst_amount_rank_5']=df_all['pst_amount_rank_5']*10//1

        #计算三种比例rank
        dolist=['open','high','low']

        for curc in dolist:
            buffer=((df_all[curc]-df_all['pre_close'])*100)/df_all['close']
            df_all[curc]=buffer
            df_all[curc]=df_all.groupby('trade_date')[curc].rank(pct=True)
            df_all[curc]=df_all[curc]*10//2

        #加入昨日rank
        df_all['yesterday_open']=df_all.groupby('ts_code')['open'].shift(1)
        df_all['yesterday_high']=df_all.groupby('ts_code')['high'].shift(1)
        df_all['yesterday_low']=df_all.groupby('ts_code')['low'].shift(1)
        df_all['yesterday_pst_amount_rank']=df_all.groupby('ts_code')['pst_amount_rank_10'].shift(1)
        ##加入前日open
        df_all['yesterday2_open']=df_all.groupby('ts_code')['open'].shift(2)
        df_all['yesterday2_high']=df_all.groupby('ts_code')['high'].shift(2)
        df_all['yesterday2_low']=df_all.groupby('ts_code')['low'].shift(2)
        df_all['yesterday2_pst_amount_rank']=df_all.groupby('ts_code')['pst_amount_rank_10'].shift(2)
        ##加入前日open
        df_all['yesterday3_open']=df_all.groupby('ts_code')['open'].shift(3)
        df_all['yesterday3_high']=df_all.groupby('ts_code')['high'].shift(3)
        df_all['yesterday3_low']=df_all.groupby('ts_code')['low'].shift(3)
        df_all['yesterday3_pst_amount_rank']=df_all.groupby('ts_code')['pst_amount_rank_10'].shift(3)


        df_all.drop(['close','pre_close','pct_chg','pst_amount','adj_factor','real_price','amount'],axis=1,inplace=True)
        #df_all.drop(['close','pre_close','pct_chg','adj_factor','real_price'],axis=1,inplace=True)

        #暂时不用的列
        df_all=df_all[df_all['high_stop']==0]

        #'tomorrow_chg'
        df_all.drop(['high_stop'],axis=1,inplace=True) 

        df_all.dropna(axis=0,how='any',inplace=True)

        print(df_all)
        df_all=df_all.reset_index(drop=True)

        return df_all

    def real_FE(self):

        df_data=pd.read_csv('real_now.csv',index_col=0,header=0)
        df_adj_all=pd.read_csv('real_adj_now.csv',index_col=0,header=0)

        df_all=pd.merge(df_data, df_adj_all, how='left', on=['ts_code','trade_date'])

        #df_all=df_all[df_all['ts_code'].str.startswith('688')==False]

        #df_all=pd.read_csv(bufferstring,index_col=0,header=0,nrows=100000)
    
        #df_all.drop(['change','vol'],axis=1,inplace=True)
 

        #===================================================================================================================================#
        
        #复权后价格
        df_all['adj_factor']=df_all['adj_factor'].fillna(0)
        df_all['real_price']=df_all['close']*df_all['adj_factor']
        
        df_all['real_price']=df_all.groupby('ts_code')['real_price'].shift(1)
        df_all['real_price']=df_all['real_price']*(1+df_all['pct_chg']/100)


        #30日最低比值
        xxx=df_all.groupby('ts_code')['real_price'].rolling(30).min().reset_index()
        xxx.set_index(['level_1'], drop=True, append=False, inplace=True, verify_integrity=False)
        xxx.drop(['ts_code'],axis=1,inplace=True)
        
        df_all=df_all.join(xxx, lsuffix='', rsuffix='_30min')
        #bbb=df_all.groupby('ts_code')['real_price'].agg({'all_min':np.min, 'all_max': np.max}).reset_index()
        #ccc=pd.merge(df_all, bbb, how='inner', on=['ts_code'])
        df_all['30_pct']=(df_all['real_price']-df_all['real_price_30min'])/df_all['real_price_30min']
        df_all['30_pct_rank']=df_all.groupby('trade_date')['30_pct'].rank(pct=True)
        df_all['30_pct_rank']=df_all['30_pct_rank']*10//2

        #30日最高比值
        xxx=df_all.groupby('ts_code')['real_price'].rolling(30).max().reset_index()
        xxx.set_index(['level_1'], drop=True, append=False, inplace=True, verify_integrity=False)
        xxx.drop(['ts_code'],axis=1,inplace=True)
        
        df_all=df_all.join(xxx, lsuffix='', rsuffix='_30max')
        #bbb=df_all.groupby('ts_code')['real_price'].agg({'all_min':np.min, 'all_max': np.max}).reset_index()
        #ccc=pd.merge(df_all, bbb, how='inner', on=['ts_code'])
        df_all['30_pct_max']=(df_all['real_price']-df_all['real_price_30max'])/df_all['real_price_30max']
        df_all['30_pct_max_rank']=df_all.groupby('trade_date')['30_pct_max'].rank(pct=True)
        df_all['30_pct_max_rank']=df_all['30_pct_max_rank']*10//2

        df_all.drop(['30_pct','real_price_30min','30_pct_max','real_price_30max'],axis=1,inplace=True)


        #===================================================================================================================================#


        #是否停
        df_all['high_stop']=0
        df_all.loc[df_all['pct_chg']>9.7,'high_stop']=1
        df_all.loc[(df_all['pct_chg']<5.2) & (4.8<df_all['pct_chg']),'high_stop']=1


        #1日
        df_all['chg_rank']=df_all.groupby('trade_date')['pct_chg'].rank(pct=True)
        df_all['chg_rank']=df_all['chg_rank']*10//2


        #6日
        xxx=df_all.groupby('ts_code')['chg_rank'].rolling(6).sum().reset_index()
        xxx.set_index(['level_1'], drop=True, append=False, inplace=True, verify_integrity=False)
        xxx.drop(['ts_code'],axis=1,inplace=True)

        df_all=df_all.join(xxx, lsuffix='', rsuffix='_6')

        df_all['chg_rank_6']=df_all.groupby('trade_date')['chg_rank_6'].rank(pct=True)
        df_all['chg_rank_6']=df_all['chg_rank_6']*10//2

        #10日
        xxx=df_all.groupby('ts_code')['chg_rank'].rolling(10).sum().reset_index()
        xxx.set_index(['level_1'], drop=True, append=False, inplace=True, verify_integrity=False)
        xxx.drop(['ts_code'],axis=1,inplace=True)

        df_all=df_all.join(xxx, lsuffix='', rsuffix='_10')

        df_all['chg_rank_10']=df_all.groupby('trade_date')['chg_rank_10'].rank(pct=True)
        df_all['chg_rank_10']=df_all['chg_rank_10']*10//2

        #3日
        xxx=df_all.groupby('ts_code')['chg_rank'].rolling(3).sum().reset_index()
        xxx.set_index(['level_1'], drop=True, append=False, inplace=True, verify_integrity=False)
        xxx.drop(['ts_code'],axis=1,inplace=True)

        df_all=df_all.join(xxx, lsuffix='', rsuffix='_3')

        df_all['chg_rank_3']=df_all.groupby('trade_date')['chg_rank_3'].rank(pct=True)
        df_all['chg_rank_3']=df_all['chg_rank_3']*10//2

        #print(df_all)

        #10日均量
        xxx=df_all.groupby('ts_code')['amount'].rolling(10).mean().reset_index()
        xxx.set_index(['level_1'], drop=True, append=False, inplace=True, verify_integrity=False)
        xxx.drop(['ts_code'],axis=1,inplace=True)
        df_all=df_all.join(xxx, lsuffix='', rsuffix='_10')

        #当日量占比
        df_all['pst_amount']=df_all['amount']/df_all['amount_10']
        df_all.drop(['amount_10'],axis=1,inplace=True)
        #当日量排名
        df_all['pst_amount_rank_10']=df_all.groupby('trade_date')['pst_amount'].rank(pct=True)
        df_all['pst_amount_rank_10']=df_all['pst_amount_rank_10']*10//2

        #计算三种比例rank
        dolist=['open','high','low']

        for curc in dolist:
            buffer=((df_all[curc]-df_all['pre_close'])*100)/df_all['pre_close']
            df_all[curc]=buffer
            df_all[curc]=df_all.groupby('trade_date')[curc].rank(pct=True)
            df_all[curc]=df_all[curc]*10//2

        #加入昨日rank
        df_all['yesterday_open']=df_all.groupby('ts_code')['open'].shift(1)
        df_all['yesterday_high']=df_all.groupby('ts_code')['high'].shift(1)
        df_all['yesterday_low']=df_all.groupby('ts_code')['low'].shift(1)
        df_all['yesterday_pst_amount_rank']=df_all.groupby('ts_code')['pst_amount_rank_10'].shift(1)
        ##加入前日open
        df_all['yesterday2_open']=df_all.groupby('ts_code')['open'].shift(2)
        df_all['yesterday2_high']=df_all.groupby('ts_code')['high'].shift(2)
        df_all['yesterday2_low']=df_all.groupby('ts_code')['low'].shift(2)
        df_all['yesterday2_pst_amount_rank']=df_all.groupby('ts_code')['pst_amount_rank_10'].shift(2)
        ##加入前日open
        df_all['yesterday3_open']=df_all.groupby('ts_code')['open'].shift(3)
        df_all['yesterday3_high']=df_all.groupby('ts_code')['high'].shift(3)
        df_all['yesterday3_low']=df_all.groupby('ts_code')['low'].shift(3)
        df_all['yesterday3_pst_amount_rank']=df_all.groupby('ts_code')['pst_amount_rank_10'].shift(3)

        df_all.drop(['close','pre_close','pct_chg','pst_amount','adj_factor','real_price','amount'],axis=1,inplace=True)



        #暂时不用的列
        df_all=df_all[df_all['high_stop']==0]
        #'tomorrow_chg'
        df_all.drop(['high_stop'],axis=1,inplace=True)



        df_all.dropna(axis=0,how='any',inplace=True)


        month_sec=df_all['trade_date'].max()
        df_all=df_all[df_all['trade_date']==month_sec]
        print(df_all)
        df_all=df_all.reset_index(drop=True)

        df_all.to_csv('today_train.csv')
        dwdw=1

class FE_h30(FEbase):
    def __init__(self):
        pass

    def core(self,DataSetName):

        df_data=pd.read_csv(DataSetName[0],index_col=0,header=0)
        df_adj_all=pd.read_csv(DataSetName[1],index_col=0,header=0)
        #df_long_all=pd.read_csv(DataSetName[2],index_col=0,header=0)

        df_all=pd.merge(df_data, df_adj_all, how='inner', on=['ts_code','trade_date'])
        #df_all=pd.merge(df_all, df_long_all, how='inner', on=['ts_code','trade_date'])

        #===================================================================================================================================#
        #df_all['pe'] = df_all['pe'].fillna(9999)
        #df_all['pb'] = df_all['pb'].fillna(9999)

        #df_all['pe_rank']=df_all.groupby('trade_date')['pe'].rank(pct=True)
        #df_all['pb_rank']=df_all.groupby('trade_date')['pb'].rank(pct=True)        
        #df_all['pe_rank']=df_all['pe_rank']*10//1
        #df_all['pb_rank']=df_all['pb_rank']*10//1

        #df_all.drop(['turnover_rate','volume_ratio','pe','pb'],axis=1,inplace=True)

        #print(df_all)
        #df_all.to_csv('sjefosia.csv')

        #===================================================================================================================================#

        ##排除科创版
        #print(df_all)
        df_all=df_all[df_all['ts_code'].str.startswith('688')==False]
        

        ##加入gap_day特征
        #start=df_all['trade_date'].apply(str)[0]
        #end=df_all['trade_date'].apply(str)[df_all.shape[0]-1]
        #xxx=pd.date_range(start,end)

        #df = pd.DataFrame(xxx)
        #df.columns = ['trade_date']
        #df['trade_date']=df['trade_date'].map(str).map(lambda x : x[:4]+x[5:7]+x[8:10]).astype("int64")

        #yyy=df_all['trade_date']
        #zzz2=yyy.unique()
        #df_2=pd.DataFrame(zzz2)
        #df_2.columns = ['trade_date']
        #df_2['day_flag']=1
    
        #result = pd.merge(df, df_2, how='left', on=['trade_date'])
        #result['day_flag2']=result['day_flag'].shift(-1)
        #result['gap_day']=0

        #result.loc[(result['day_flag']==1) & (result['day_flag2']!=1),'gap_day']=1

        #result.drop(['day_flag','day_flag2'],axis=1,inplace=True)

        #df_all=pd.merge(df_all, result, how='left', on=['trade_date'])

        #===================================================================================================================================#

        #复权后价格
        df_all['real_price']=df_all['close']*df_all['adj_factor']
        
        #30日最低比值
        xxx=df_all.groupby('ts_code')['real_price'].rolling(30).min().reset_index()
        xxx.set_index(['level_1'], drop=True, append=False, inplace=True, verify_integrity=False)
        xxx.drop(['ts_code'],axis=1,inplace=True)
        
        df_all=df_all.join(xxx, lsuffix='', rsuffix='_30min')
        #bbb=df_all.groupby('ts_code')['real_price'].agg({'all_min':np.min, 'all_max': np.max}).reset_index()
        #ccc=pd.merge(df_all, bbb, how='inner', on=['ts_code'])
        df_all['30_pct']=(df_all['real_price']-df_all['real_price_30min'])/df_all['real_price_30min']
        df_all['30_pct_rank']=df_all.groupby('trade_date')['30_pct'].rank(pct=True)
        df_all['30_pct_rank']=df_all['30_pct_rank']*10//2

        #30日最高比值
        xxx=df_all.groupby('ts_code')['real_price'].rolling(30).max().reset_index()
        xxx.set_index(['level_1'], drop=True, append=False, inplace=True, verify_integrity=False)
        xxx.drop(['ts_code'],axis=1,inplace=True)
        
        df_all=df_all.join(xxx, lsuffix='', rsuffix='_30max')
        #bbb=df_all.groupby('ts_code')['real_price'].agg({'all_min':np.min, 'all_max': np.max}).reset_index()
        #ccc=pd.merge(df_all, bbb, how='inner', on=['ts_code'])
        df_all['30_pct_max']=(df_all['real_price']-df_all['real_price_30max'])/df_all['real_price_30max']
        df_all['30_pct_max_rank']=df_all.groupby('trade_date')['30_pct_max'].rank(pct=True)
        df_all['30_pct_max_rank']=df_all['30_pct_max_rank']*10//2



        df_all.drop(['30_pct','real_price_30min','30_pct_max','real_price_30max'],axis=1,inplace=True)
        #df_all.drop(['20_pct','real_price_20min','20_pct_max','real_price_20max'],axis=1,inplace=True)

        df_all.drop(['change','vol'],axis=1,inplace=True)

        #===================================================================================================================================#
    
        #明日幅度
        tm1=df_all.groupby('ts_code')['pct_chg'].shift(-1)
        tm2=df_all.groupby('ts_code')['pct_chg'].shift(-2)
        #df_all['tomorrow_chg']=((100+tm1)*(100+tm2)-10000)/100
        tm3=df_all.groupby('ts_code')['pct_chg'].shift(-3)
        tm4=df_all.groupby('ts_code')['pct_chg'].shift(-4)
        tm5=df_all.groupby('ts_code')['pct_chg'].shift(-5)

        df_all['tomorrow_chg']=(((100+tm1)/100)*((100+tm2)/100)*((100+tm3)/100)*((100+tm4)/100)*((100+tm5)/100)-1)*100

        #df_all['tomorrow_chg']=((100+tm1)*(100+tm2)*(100+tm3)-1000000)/10000
        #df_all['tomorrow_chg']=df_all.groupby('ts_code')['pct_chg'].shift(-1)
        #明日排名
        df_all['tomorrow_chg_rank']=df_all.groupby('trade_date')['tomorrow_chg'].rank(pct=True)
        df_all['tomorrow_chg_rank']=df_all['tomorrow_chg_rank']*9.9//1
        #是否停
        df_all['high_stop']=0
        df_all.loc[df_all['pct_chg']>9.4,'high_stop']=1
        df_all.loc[(df_all['pct_chg']<5.2) & (4.8<df_all['pct_chg']),'high_stop']=1


        ###真实价格范围(区分实际股价高低)
        #df_all['price_real_rank']=df_all.groupby('trade_date')['pre_close'].rank(pct=True)
        #df_all['price_real_rank']=df_all['price_real_rank']*10//1
        #1日
        df_all['chg_rank']=df_all.groupby('trade_date')['pct_chg'].rank(pct=True)


        #6日
        xxx=df_all.groupby('ts_code')['chg_rank'].rolling(6).sum().reset_index()
        xxx.set_index(['level_1'], drop=True, append=False, inplace=True, verify_integrity=False)
        xxx.drop(['ts_code'],axis=1,inplace=True)

        df_all=df_all.join(xxx, lsuffix='', rsuffix='_6')

        df_all['chg_rank_6']=df_all.groupby('trade_date')['chg_rank_6'].rank(pct=True)
        df_all['chg_rank_6']=df_all['chg_rank_6']*10//2

        #10日
        xxx=df_all.groupby('ts_code')['chg_rank'].rolling(10).sum().reset_index()
        xxx.set_index(['level_1'], drop=True, append=False, inplace=True, verify_integrity=False)
        xxx.drop(['ts_code'],axis=1,inplace=True)

        df_all=df_all.join(xxx, lsuffix='', rsuffix='_10')

        df_all['chg_rank_10']=df_all.groupby('trade_date')['chg_rank_10'].rank(pct=True)
        df_all['chg_rank_10']=df_all['chg_rank_10']*10//2

        #3日
        xxx=df_all.groupby('ts_code')['chg_rank'].rolling(3).sum().reset_index()
        xxx.set_index(['level_1'], drop=True, append=False, inplace=True, verify_integrity=False)
        xxx.drop(['ts_code'],axis=1,inplace=True)

        df_all=df_all.join(xxx, lsuffix='', rsuffix='_3')

        df_all['chg_rank_3']=df_all.groupby('trade_date')['chg_rank_3'].rank(pct=True)
        df_all['chg_rank_3']=df_all['chg_rank_3']*10//2

        #5日max
        xxx=df_all.groupby('ts_code')['chg_rank'].rolling(5).max().reset_index()
        xxx.set_index(['level_1'], drop=True, append=False, inplace=True, verify_integrity=False)
        xxx.drop(['ts_code'],axis=1,inplace=True)

        df_all=df_all.join(xxx, lsuffix='', rsuffix='_max_5')

        df_all['chg_rank_max_5']=df_all.groupby('trade_date')['chg_rank_max_5'].rank(pct=True)
        df_all['chg_rank_max_5']=df_all['chg_rank_max_5']*10//2

        #5日min
        xxx=df_all.groupby('ts_code')['chg_rank'].rolling(5).min().reset_index()
        xxx.set_index(['level_1'], drop=True, append=False, inplace=True, verify_integrity=False)
        xxx.drop(['ts_code'],axis=1,inplace=True)

        df_all=df_all.join(xxx, lsuffix='', rsuffix='_min_5')

        df_all['chg_rank_min_5']=df_all.groupby('trade_date')['chg_rank_min_5'].rank(pct=True)
        df_all['chg_rank_min_5']=df_all['chg_rank_min_5']*10//2


        #将1日的也粗略化
        df_all['chg_rank']=df_all['chg_rank']*10//2

        ##20日
        #xxx=df_all.groupby('ts_code')['chg_rank'].rolling(20).sum().reset_index()
        #xxx.set_index(['level_1'], drop=True, append=False, inplace=True, verify_integrity=False)
        #xxx.drop(['ts_code'],axis=1,inplace=True)

        #df_all=df_all.join(xxx, lsuffix='', rsuffix='_20')

        #df_all['chg_rank_20']=df_all.groupby('trade_date')['chg_rank_3'].rank(pct=True)
        #df_all['chg_rank_20']=df_all['chg_rank_20']*10//1

        #print(df_all)

        #10日均量
        xxx=df_all.groupby('ts_code')['amount'].rolling(10).mean().reset_index()
        xxx.set_index(['level_1'], drop=True, append=False, inplace=True, verify_integrity=False)
        xxx.drop(['ts_code'],axis=1,inplace=True)
        df_all=df_all.join(xxx, lsuffix='', rsuffix='_10')

        #当日量占比
        df_all['pst_amount']=df_all['amount']/df_all['amount_10']
        df_all.drop(['amount_10'],axis=1,inplace=True)
        #当日量排名
        df_all['pst_amount_rank_10']=df_all.groupby('trade_date')['pst_amount'].rank(pct=True)
        df_all['pst_amount_rank_10']=df_all['pst_amount_rank_10']*10//2

        ##5日均量
        #xxx=df_all.groupby('ts_code')['amount'].rolling(5).mean().reset_index()
        #xxx.set_index(['level_1'], drop=True, append=False, inplace=True, verify_integrity=False)
        #xxx.drop(['ts_code'],axis=1,inplace=True)
        #df_all=df_all.join(xxx, lsuffix='', rsuffix='_5')

        ##当日量占比
        #df_all['pst_amount']=df_all['amount']/df_all['amount_5']
        #df_all.drop(['amount_5'],axis=1,inplace=True)
        ##当日量排名
        #df_all['pst_amount_rank_5']=df_all.groupby('trade_date')['pst_amount'].rank(pct=True)
        #df_all['pst_amount_rank_5']=df_all['pst_amount_rank_5']*10//1

        #计算三种比例rank
        dolist=['open','high','low']

        for curc in dolist:
            buffer=((df_all[curc]-df_all['pre_close'])*100)/df_all['close']
            df_all[curc]=buffer
            df_all[curc]=df_all.groupby('trade_date')[curc].rank(pct=True)
            df_all[curc]=df_all[curc]*10//2

        #加入昨日rank
        df_all['yesterday_open']=df_all.groupby('ts_code')['open'].shift(1)
        df_all['yesterday_high']=df_all.groupby('ts_code')['high'].shift(1)
        df_all['yesterday_low']=df_all.groupby('ts_code')['low'].shift(1)
        df_all['yesterday_pst_amount_rank']=df_all.groupby('ts_code')['pst_amount_rank_10'].shift(1)
        ##加入前日open
        df_all['yesterday2_open']=df_all.groupby('ts_code')['open'].shift(2)
        df_all['yesterday2_high']=df_all.groupby('ts_code')['high'].shift(2)
        df_all['yesterday2_low']=df_all.groupby('ts_code')['low'].shift(2)
        df_all['yesterday2_pst_amount_rank']=df_all.groupby('ts_code')['pst_amount_rank_10'].shift(2)
        ##加入前日open
        df_all['yesterday3_open']=df_all.groupby('ts_code')['open'].shift(3)
        df_all['yesterday3_high']=df_all.groupby('ts_code')['high'].shift(3)
        df_all['yesterday3_low']=df_all.groupby('ts_code')['low'].shift(3)
        df_all['yesterday3_pst_amount_rank']=df_all.groupby('ts_code')['pst_amount_rank_10'].shift(3)


        df_all.drop(['close','pre_close','pct_chg','pst_amount','adj_factor','real_price','amount'],axis=1,inplace=True)
        #df_all.drop(['close','pre_close','pct_chg','adj_factor','real_price'],axis=1,inplace=True)

        #暂时不用的列
        df_all=df_all[df_all['high_stop']==0]

        #'tomorrow_chg'
        df_all.drop(['high_stop'],axis=1,inplace=True) 

        df_all.dropna(axis=0,how='any',inplace=True)

        print(df_all)
        df_all=df_all.reset_index(drop=True)

        return df_all

    def real_FE(self):

        df_data=pd.read_csv('real_now.csv',index_col=0,header=0)
        df_adj_all=pd.read_csv('real_adj_now.csv',index_col=0,header=0)

        df_all=pd.merge(df_data, df_adj_all, how='left', on=['ts_code','trade_date'])

        #df_all=df_all[df_all['ts_code'].str.startswith('688')==False]

        #df_all=pd.read_csv(bufferstring,index_col=0,header=0,nrows=100000)
    
        #df_all.drop(['change','vol'],axis=1,inplace=True)
 

        #===================================================================================================================================#
        
        #复权后价格
        df_all['adj_factor']=df_all['adj_factor'].fillna(0)
        df_all['real_price']=df_all['close']*df_all['adj_factor']
        
        df_all['real_price']=df_all.groupby('ts_code')['real_price'].shift(1)
        df_all['real_price']=df_all['real_price']*(1+df_all['pct_chg']/100)


        #30日最低比值
        xxx=df_all.groupby('ts_code')['real_price'].rolling(30).min().reset_index()
        xxx.set_index(['level_1'], drop=True, append=False, inplace=True, verify_integrity=False)
        xxx.drop(['ts_code'],axis=1,inplace=True)
        
        df_all=df_all.join(xxx, lsuffix='', rsuffix='_30min')
        #bbb=df_all.groupby('ts_code')['real_price'].agg({'all_min':np.min, 'all_max': np.max}).reset_index()
        #ccc=pd.merge(df_all, bbb, how='inner', on=['ts_code'])
        df_all['30_pct']=(df_all['real_price']-df_all['real_price_30min'])/df_all['real_price_30min']
        df_all['30_pct_rank']=df_all.groupby('trade_date')['30_pct'].rank(pct=True)
        df_all['30_pct_rank']=df_all['30_pct_rank']*10//2

        #30日最高比值
        xxx=df_all.groupby('ts_code')['real_price'].rolling(30).max().reset_index()
        xxx.set_index(['level_1'], drop=True, append=False, inplace=True, verify_integrity=False)
        xxx.drop(['ts_code'],axis=1,inplace=True)
        
        df_all=df_all.join(xxx, lsuffix='', rsuffix='_30max')
        #bbb=df_all.groupby('ts_code')['real_price'].agg({'all_min':np.min, 'all_max': np.max}).reset_index()
        #ccc=pd.merge(df_all, bbb, how='inner', on=['ts_code'])
        df_all['30_pct_max']=(df_all['real_price']-df_all['real_price_30max'])/df_all['real_price_30max']
        df_all['30_pct_max_rank']=df_all.groupby('trade_date')['30_pct_max'].rank(pct=True)
        df_all['30_pct_max_rank']=df_all['30_pct_max_rank']*10//2

        df_all.drop(['30_pct','real_price_30min','30_pct_max','real_price_30max'],axis=1,inplace=True)


        #===================================================================================================================================#


        #是否停
        df_all['high_stop']=0
        df_all.loc[df_all['pct_chg']>9.7,'high_stop']=1
        df_all.loc[(df_all['pct_chg']<5.2) & (4.8<df_all['pct_chg']),'high_stop']=1


        #1日
        df_all['chg_rank']=df_all.groupby('trade_date')['pct_chg'].rank(pct=True)
        df_all['chg_rank']=df_all['chg_rank']*10//2


        #6日
        xxx=df_all.groupby('ts_code')['chg_rank'].rolling(6).sum().reset_index()
        xxx.set_index(['level_1'], drop=True, append=False, inplace=True, verify_integrity=False)
        xxx.drop(['ts_code'],axis=1,inplace=True)

        df_all=df_all.join(xxx, lsuffix='', rsuffix='_6')

        df_all['chg_rank_6']=df_all.groupby('trade_date')['chg_rank_6'].rank(pct=True)
        df_all['chg_rank_6']=df_all['chg_rank_6']*10//2

        #10日
        xxx=df_all.groupby('ts_code')['chg_rank'].rolling(10).sum().reset_index()
        xxx.set_index(['level_1'], drop=True, append=False, inplace=True, verify_integrity=False)
        xxx.drop(['ts_code'],axis=1,inplace=True)

        df_all=df_all.join(xxx, lsuffix='', rsuffix='_10')

        df_all['chg_rank_10']=df_all.groupby('trade_date')['chg_rank_10'].rank(pct=True)
        df_all['chg_rank_10']=df_all['chg_rank_10']*10//2

        #3日
        xxx=df_all.groupby('ts_code')['chg_rank'].rolling(3).sum().reset_index()
        xxx.set_index(['level_1'], drop=True, append=False, inplace=True, verify_integrity=False)
        xxx.drop(['ts_code'],axis=1,inplace=True)

        df_all=df_all.join(xxx, lsuffix='', rsuffix='_3')

        df_all['chg_rank_3']=df_all.groupby('trade_date')['chg_rank_3'].rank(pct=True)
        df_all['chg_rank_3']=df_all['chg_rank_3']*10//2

        #print(df_all)

        #10日均量
        xxx=df_all.groupby('ts_code')['amount'].rolling(10).mean().reset_index()
        xxx.set_index(['level_1'], drop=True, append=False, inplace=True, verify_integrity=False)
        xxx.drop(['ts_code'],axis=1,inplace=True)
        df_all=df_all.join(xxx, lsuffix='', rsuffix='_10')

        #当日量占比
        df_all['pst_amount']=df_all['amount']/df_all['amount_10']
        df_all.drop(['amount_10'],axis=1,inplace=True)
        #当日量排名
        df_all['pst_amount_rank_10']=df_all.groupby('trade_date')['pst_amount'].rank(pct=True)
        df_all['pst_amount_rank_10']=df_all['pst_amount_rank_10']*10//2

        #计算三种比例rank
        dolist=['open','high','low']

        for curc in dolist:
            buffer=((df_all[curc]-df_all['pre_close'])*100)/df_all['pre_close']
            df_all[curc]=buffer
            df_all[curc]=df_all.groupby('trade_date')[curc].rank(pct=True)
            df_all[curc]=df_all[curc]*10//2

        #加入昨日rank
        df_all['yesterday_open']=df_all.groupby('ts_code')['open'].shift(1)
        df_all['yesterday_high']=df_all.groupby('ts_code')['high'].shift(1)
        df_all['yesterday_low']=df_all.groupby('ts_code')['low'].shift(1)
        df_all['yesterday_pst_amount_rank']=df_all.groupby('ts_code')['pst_amount_rank_10'].shift(1)
        ##加入前日open
        df_all['yesterday2_open']=df_all.groupby('ts_code')['open'].shift(2)
        df_all['yesterday2_high']=df_all.groupby('ts_code')['high'].shift(2)
        df_all['yesterday2_low']=df_all.groupby('ts_code')['low'].shift(2)
        df_all['yesterday2_pst_amount_rank']=df_all.groupby('ts_code')['pst_amount_rank_10'].shift(2)
        ##加入前日open
        df_all['yesterday3_open']=df_all.groupby('ts_code')['open'].shift(3)
        df_all['yesterday3_high']=df_all.groupby('ts_code')['high'].shift(3)
        df_all['yesterday3_low']=df_all.groupby('ts_code')['low'].shift(3)
        df_all['yesterday3_pst_amount_rank']=df_all.groupby('ts_code')['pst_amount_rank_10'].shift(3)

        df_all.drop(['close','pre_close','pct_chg','pst_amount','adj_factor','real_price','amount'],axis=1,inplace=True)



        #暂时不用的列
        df_all=df_all[df_all['high_stop']==0]
        #'tomorrow_chg'
        df_all.drop(['high_stop'],axis=1,inplace=True)



        df_all.dropna(axis=0,how='any',inplace=True)


        month_sec=df_all['trade_date'].max()
        df_all=df_all[df_all['trade_date']==month_sec]
        print(df_all)
        df_all=df_all.reset_index(drop=True)

        df_all.to_csv('today_train.csv')
        dwdw=1

class FEg30_highstop(FEbase):
    def __init__(self):
        pass

    def core(self,DataSetName):

        df_data=pd.read_csv(DataSetName[0],index_col=0,header=0)
        df_adj_all=pd.read_csv(DataSetName[1],index_col=0,header=0)
        #df_long_all=pd.read_csv(DataSetName[2],index_col=0,header=0)

        df_all=pd.merge(df_data, df_adj_all, how='inner', on=['ts_code','trade_date'])
        #df_all=pd.merge(df_all, df_long_all, how='inner', on=['ts_code','trade_date'])

        #===================================================================================================================================#
        #df_all['pe'] = df_all['pe'].fillna(9999)
        #df_all['pb'] = df_all['pb'].fillna(9999)

        #df_all['pe_rank']=df_all.groupby('trade_date')['pe'].rank(pct=True)
        #df_all['pb_rank']=df_all.groupby('trade_date')['pb'].rank(pct=True)        
        #df_all['pe_rank']=df_all['pe_rank']*10//1
        #df_all['pb_rank']=df_all['pb_rank']*10//1

        #df_all.drop(['turnover_rate','volume_ratio','pe','pb'],axis=1,inplace=True)

        #print(df_all)
        #df_all.to_csv('sjefosia.csv')

        #===================================================================================================================================#

        ##排除科创版
        #print(df_all)
        df_all=df_all[df_all['ts_code'].str.startswith('688')==False]
        

        ##加入gap_day特征
        #start=df_all['trade_date'].apply(str)[0]
        #end=df_all['trade_date'].apply(str)[df_all.shape[0]-1]
        #xxx=pd.date_range(start,end)

        #df = pd.DataFrame(xxx)
        #df.columns = ['trade_date']
        #df['trade_date']=df['trade_date'].map(str).map(lambda x : x[:4]+x[5:7]+x[8:10]).astype("int64")

        #yyy=df_all['trade_date']
        #zzz2=yyy.unique()
        #df_2=pd.DataFrame(zzz2)
        #df_2.columns = ['trade_date']
        #df_2['day_flag']=1
    
        #result = pd.merge(df, df_2, how='left', on=['trade_date'])
        #result['day_flag2']=result['day_flag'].shift(-1)
        #result['gap_day']=0

        #result.loc[(result['day_flag']==1) & (result['day_flag2']!=1),'gap_day']=1

        #result.drop(['day_flag','day_flag2'],axis=1,inplace=True)

        #df_all=pd.merge(df_all, result, how='left', on=['trade_date'])

        #===================================================================================================================================#

        #复权后价格
        df_all['real_price']=df_all['close']*df_all['adj_factor']
        
        #30日最低比值
        xxx=df_all.groupby('ts_code')['real_price'].rolling(30).min().reset_index()
        xxx.set_index(['level_1'], drop=True, append=False, inplace=True, verify_integrity=False)
        xxx.drop(['ts_code'],axis=1,inplace=True)
        
        df_all=df_all.join(xxx, lsuffix='', rsuffix='_30min')
        #bbb=df_all.groupby('ts_code')['real_price'].agg({'all_min':np.min, 'all_max': np.max}).reset_index()
        #ccc=pd.merge(df_all, bbb, how='inner', on=['ts_code'])
        df_all['30_pct']=(df_all['real_price']-df_all['real_price_30min'])/df_all['real_price_30min']
        df_all['30_pct_rank']=df_all.groupby('trade_date')['30_pct'].rank(pct=True)
        df_all['30_pct_rank']=df_all['30_pct_rank']*10//2

        #30日最高比值
        xxx=df_all.groupby('ts_code')['real_price'].rolling(30).max().reset_index()
        xxx.set_index(['level_1'], drop=True, append=False, inplace=True, verify_integrity=False)
        xxx.drop(['ts_code'],axis=1,inplace=True)
        
        df_all=df_all.join(xxx, lsuffix='', rsuffix='_30max')
        #bbb=df_all.groupby('ts_code')['real_price'].agg({'all_min':np.min, 'all_max': np.max}).reset_index()
        #ccc=pd.merge(df_all, bbb, how='inner', on=['ts_code'])
        df_all['30_pct_max']=(df_all['real_price']-df_all['real_price_30max'])/df_all['real_price_30max']
        df_all['30_pct_max_rank']=df_all.groupby('trade_date')['30_pct_max'].rank(pct=True)
        df_all['30_pct_max_rank']=df_all['30_pct_max_rank']*10//2



        df_all.drop(['30_pct','real_price_30min','30_pct_max','real_price_30max'],axis=1,inplace=True)
        #df_all.drop(['20_pct','real_price_20min','20_pct_max','real_price_20max'],axis=1,inplace=True)

        df_all.drop(['change','vol'],axis=1,inplace=True)

        #===================================================================================================================================#
    
        #明日幅度
        #tm1=df_all.groupby('ts_code')['pct_chg'].shift(-1)
        #tm2=df_all.groupby('ts_code')['pct_chg'].shift(-2)
        ##df_all['tomorrow_chg']=((100+tm1)*(100+tm2)-10000)/100
        #tm3=df_all.groupby('ts_code')['pct_chg'].shift(-3)
        #tm4=df_all.groupby('ts_code')['pct_chg'].shift(-4)
        #tm5=df_all.groupby('ts_code')['pct_chg'].shift(-5)

        #df_all['tomorrow_chg']=(((100+tm1)/100)*((100+tm2)/100)*((100+tm3)/100)*((100+tm4)/100)*((100+tm5)/100)-1)*100

        #df_all['tomorrow_chg']=((100+tm1)*(100+tm2)*(100+tm3)-1000000)/10000
        df_all['tomorrow_chg']=df_all.groupby('ts_code')['pct_chg'].shift(-1)
        #明日排名
        df_all['tomorrow_chg_rank']=df_all.groupby('trade_date')['tomorrow_chg'].rank(pct=True)
        df_all['tomorrow_chg_rank']=df_all['tomorrow_chg_rank']*9.9//1
        #是否停
        df_all['high_stop']=0
        df_all.loc[df_all['pct_chg']>9.4,'high_stop']=1
        df_all.loc[(df_all['pct_chg']<5.2) & (4.8<df_all['pct_chg']),'high_stop']=1
        df_all['tomorrow_high_stop']=0
        df_all.loc[df_all['tomorrow_chg_rank']>8,'tomorrow_high_stop']=1

        ###真实价格范围(区分实际股价高低)
        #df_all['price_real_rank']=df_all.groupby('trade_date')['pre_close'].rank(pct=True)
        #df_all['price_real_rank']=df_all['price_real_rank']*10//1
        #1日
        df_all['chg_rank']=df_all.groupby('trade_date')['pct_chg'].rank(pct=True)
        df_all['chg_rank']=df_all['chg_rank']*10//2


        #6日
        xxx=df_all.groupby('ts_code')['chg_rank'].rolling(6).sum().reset_index()
        xxx.set_index(['level_1'], drop=True, append=False, inplace=True, verify_integrity=False)
        xxx.drop(['ts_code'],axis=1,inplace=True)

        df_all=df_all.join(xxx, lsuffix='', rsuffix='_6')

        df_all['chg_rank_6']=df_all.groupby('trade_date')['chg_rank_6'].rank(pct=True)
        df_all['chg_rank_6']=df_all['chg_rank_6']*10//2

        #10日
        xxx=df_all.groupby('ts_code')['chg_rank'].rolling(10).sum().reset_index()
        xxx.set_index(['level_1'], drop=True, append=False, inplace=True, verify_integrity=False)
        xxx.drop(['ts_code'],axis=1,inplace=True)

        df_all=df_all.join(xxx, lsuffix='', rsuffix='_10')

        df_all['chg_rank_10']=df_all.groupby('trade_date')['chg_rank_10'].rank(pct=True)
        df_all['chg_rank_10']=df_all['chg_rank_10']*10//2

        #3日
        xxx=df_all.groupby('ts_code')['chg_rank'].rolling(3).sum().reset_index()
        xxx.set_index(['level_1'], drop=True, append=False, inplace=True, verify_integrity=False)
        xxx.drop(['ts_code'],axis=1,inplace=True)

        df_all=df_all.join(xxx, lsuffix='', rsuffix='_3')

        df_all['chg_rank_3']=df_all.groupby('trade_date')['chg_rank_3'].rank(pct=True)
        df_all['chg_rank_3']=df_all['chg_rank_3']*10//2

        ##20日
        #xxx=df_all.groupby('ts_code')['chg_rank'].rolling(20).sum().reset_index()
        #xxx.set_index(['level_1'], drop=True, append=False, inplace=True, verify_integrity=False)
        #xxx.drop(['ts_code'],axis=1,inplace=True)

        #df_all=df_all.join(xxx, lsuffix='', rsuffix='_20')

        #df_all['chg_rank_20']=df_all.groupby('trade_date')['chg_rank_3'].rank(pct=True)
        #df_all['chg_rank_20']=df_all['chg_rank_20']*10//1

        #print(df_all)

        #10日均量
        xxx=df_all.groupby('ts_code')['amount'].rolling(10).mean().reset_index()
        xxx.set_index(['level_1'], drop=True, append=False, inplace=True, verify_integrity=False)
        xxx.drop(['ts_code'],axis=1,inplace=True)
        df_all=df_all.join(xxx, lsuffix='', rsuffix='_10')

        #当日量占比
        df_all['pst_amount']=df_all['amount']/df_all['amount_10']
        df_all.drop(['amount_10'],axis=1,inplace=True)
        #当日量排名
        df_all['pst_amount_rank_10']=df_all.groupby('trade_date')['pst_amount'].rank(pct=True)
        df_all['pst_amount_rank_10']=df_all['pst_amount_rank_10']*10//2

        ##5日均量
        #xxx=df_all.groupby('ts_code')['amount'].rolling(5).mean().reset_index()
        #xxx.set_index(['level_1'], drop=True, append=False, inplace=True, verify_integrity=False)
        #xxx.drop(['ts_code'],axis=1,inplace=True)
        #df_all=df_all.join(xxx, lsuffix='', rsuffix='_5')

        ##当日量占比
        #df_all['pst_amount']=df_all['amount']/df_all['amount_5']
        #df_all.drop(['amount_5'],axis=1,inplace=True)
        ##当日量排名
        #df_all['pst_amount_rank_5']=df_all.groupby('trade_date')['pst_amount'].rank(pct=True)
        #df_all['pst_amount_rank_5']=df_all['pst_amount_rank_5']*10//1

        #计算三种比例rank
        dolist=['open','high','low']

        for curc in dolist:
            buffer=((df_all[curc]-df_all['pre_close'])*100)/df_all['close']
            df_all[curc]=buffer
            df_all[curc]=df_all.groupby('trade_date')[curc].rank(pct=True)
            df_all[curc]=df_all[curc]*10//2

        #加入昨日rank
        df_all['yesterday_open']=df_all.groupby('ts_code')['open'].shift(1)
        df_all['yesterday_high']=df_all.groupby('ts_code')['high'].shift(1)
        df_all['yesterday_low']=df_all.groupby('ts_code')['low'].shift(1)
        df_all['yesterday_pst_amount_rank']=df_all.groupby('ts_code')['pst_amount_rank_10'].shift(1)
        ##加入前日open
        df_all['yesterday2_open']=df_all.groupby('ts_code')['open'].shift(2)
        df_all['yesterday2_high']=df_all.groupby('ts_code')['high'].shift(2)
        df_all['yesterday2_low']=df_all.groupby('ts_code')['low'].shift(2)
        df_all['yesterday2_pst_amount_rank']=df_all.groupby('ts_code')['pst_amount_rank_10'].shift(2)
        ##加入前日open
        df_all['yesterday3_open']=df_all.groupby('ts_code')['open'].shift(3)
        df_all['yesterday3_high']=df_all.groupby('ts_code')['high'].shift(3)
        df_all['yesterday3_low']=df_all.groupby('ts_code')['low'].shift(3)
        df_all['yesterday3_pst_amount_rank']=df_all.groupby('ts_code')['pst_amount_rank_10'].shift(3)


        df_all.drop(['close','pre_close','pct_chg','pst_amount','adj_factor','real_price','amount'],axis=1,inplace=True)
        #df_all.drop(['close','pre_close','pct_chg','adj_factor','real_price'],axis=1,inplace=True)

        #暂时不用的列
        df_all=df_all[df_all['high_stop']==0]

        #'tomorrow_chg'
        df_all.drop(['high_stop'],axis=1,inplace=True) 

        df_all.dropna(axis=0,how='any',inplace=True)

        print(df_all)
        df_all=df_all.reset_index(drop=True)

        return df_all

    def real_FE(self):

        df_data=pd.read_csv('real_now.csv',index_col=0,header=0)
        df_adj_all=pd.read_csv('real_adj_now.csv',index_col=0,header=0)

        df_all=pd.merge(df_data, df_adj_all, how='left', on=['ts_code','trade_date'])

        #df_all=df_all[df_all['ts_code'].str.startswith('688')==False]

        #df_all=pd.read_csv(bufferstring,index_col=0,header=0,nrows=100000)
    
        #df_all.drop(['change','vol'],axis=1,inplace=True)
 

        #===================================================================================================================================#
        
        #复权后价格
        df_all['adj_factor']=df_all['adj_factor'].fillna(0)
        df_all['real_price']=df_all['close']*df_all['adj_factor']
        
        df_all['real_price']=df_all.groupby('ts_code')['real_price'].shift(1)
        df_all['real_price']=df_all['real_price']*(1+df_all['pct_chg']/100)


        #30日最低比值
        xxx=df_all.groupby('ts_code')['real_price'].rolling(30).min().reset_index()
        xxx.set_index(['level_1'], drop=True, append=False, inplace=True, verify_integrity=False)
        xxx.drop(['ts_code'],axis=1,inplace=True)
        
        df_all=df_all.join(xxx, lsuffix='', rsuffix='_30min')
        #bbb=df_all.groupby('ts_code')['real_price'].agg({'all_min':np.min, 'all_max': np.max}).reset_index()
        #ccc=pd.merge(df_all, bbb, how='inner', on=['ts_code'])
        df_all['30_pct']=(df_all['real_price']-df_all['real_price_30min'])/df_all['real_price_30min']
        df_all['30_pct_rank']=df_all.groupby('trade_date')['30_pct'].rank(pct=True)
        df_all['30_pct_rank']=df_all['30_pct_rank']*10//2

        #30日最高比值
        xxx=df_all.groupby('ts_code')['real_price'].rolling(30).max().reset_index()
        xxx.set_index(['level_1'], drop=True, append=False, inplace=True, verify_integrity=False)
        xxx.drop(['ts_code'],axis=1,inplace=True)
        
        df_all=df_all.join(xxx, lsuffix='', rsuffix='_30max')
        #bbb=df_all.groupby('ts_code')['real_price'].agg({'all_min':np.min, 'all_max': np.max}).reset_index()
        #ccc=pd.merge(df_all, bbb, how='inner', on=['ts_code'])
        df_all['30_pct_max']=(df_all['real_price']-df_all['real_price_30max'])/df_all['real_price_30max']
        df_all['30_pct_max_rank']=df_all.groupby('trade_date')['30_pct_max'].rank(pct=True)
        df_all['30_pct_max_rank']=df_all['30_pct_max_rank']*10//2

        df_all.drop(['30_pct','real_price_30min','30_pct_max','real_price_30max'],axis=1,inplace=True)


        #===================================================================================================================================#


        #是否停
        df_all['high_stop']=0
        df_all.loc[df_all['pct_chg']>9.7,'high_stop']=1
        df_all.loc[(df_all['pct_chg']<5.2) & (4.8<df_all['pct_chg']),'high_stop']=1


        #1日
        df_all['chg_rank']=df_all.groupby('trade_date')['pct_chg'].rank(pct=True)
        df_all['chg_rank']=df_all['chg_rank']*10//2


        #6日
        xxx=df_all.groupby('ts_code')['chg_rank'].rolling(6).sum().reset_index()
        xxx.set_index(['level_1'], drop=True, append=False, inplace=True, verify_integrity=False)
        xxx.drop(['ts_code'],axis=1,inplace=True)

        df_all=df_all.join(xxx, lsuffix='', rsuffix='_6')

        df_all['chg_rank_6']=df_all.groupby('trade_date')['chg_rank_6'].rank(pct=True)
        df_all['chg_rank_6']=df_all['chg_rank_6']*10//2

        #10日
        xxx=df_all.groupby('ts_code')['chg_rank'].rolling(10).sum().reset_index()
        xxx.set_index(['level_1'], drop=True, append=False, inplace=True, verify_integrity=False)
        xxx.drop(['ts_code'],axis=1,inplace=True)

        df_all=df_all.join(xxx, lsuffix='', rsuffix='_10')

        df_all['chg_rank_10']=df_all.groupby('trade_date')['chg_rank_10'].rank(pct=True)
        df_all['chg_rank_10']=df_all['chg_rank_10']*10//2

        #3日
        xxx=df_all.groupby('ts_code')['chg_rank'].rolling(3).sum().reset_index()
        xxx.set_index(['level_1'], drop=True, append=False, inplace=True, verify_integrity=False)
        xxx.drop(['ts_code'],axis=1,inplace=True)

        df_all=df_all.join(xxx, lsuffix='', rsuffix='_3')

        df_all['chg_rank_3']=df_all.groupby('trade_date')['chg_rank_3'].rank(pct=True)
        df_all['chg_rank_3']=df_all['chg_rank_3']*10//2

        #print(df_all)

        #10日均量
        xxx=df_all.groupby('ts_code')['amount'].rolling(10).mean().reset_index()
        xxx.set_index(['level_1'], drop=True, append=False, inplace=True, verify_integrity=False)
        xxx.drop(['ts_code'],axis=1,inplace=True)
        df_all=df_all.join(xxx, lsuffix='', rsuffix='_10')

        #当日量占比
        df_all['pst_amount']=df_all['amount']/df_all['amount_10']
        df_all.drop(['amount_10'],axis=1,inplace=True)
        #当日量排名
        df_all['pst_amount_rank_10']=df_all.groupby('trade_date')['pst_amount'].rank(pct=True)
        df_all['pst_amount_rank_10']=df_all['pst_amount_rank_10']*10//2

        #计算三种比例rank
        dolist=['open','high','low']

        for curc in dolist:
            buffer=((df_all[curc]-df_all['pre_close'])*100)/df_all['pre_close']
            df_all[curc]=buffer
            df_all[curc]=df_all.groupby('trade_date')[curc].rank(pct=True)
            df_all[curc]=df_all[curc]*10//2

        #加入昨日rank
        df_all['yesterday_open']=df_all.groupby('ts_code')['open'].shift(1)
        df_all['yesterday_high']=df_all.groupby('ts_code')['high'].shift(1)
        df_all['yesterday_low']=df_all.groupby('ts_code')['low'].shift(1)
        df_all['yesterday_pst_amount_rank']=df_all.groupby('ts_code')['pst_amount_rank_10'].shift(1)
        ##加入前日open
        df_all['yesterday2_open']=df_all.groupby('ts_code')['open'].shift(2)
        df_all['yesterday2_high']=df_all.groupby('ts_code')['high'].shift(2)
        df_all['yesterday2_low']=df_all.groupby('ts_code')['low'].shift(2)
        df_all['yesterday2_pst_amount_rank']=df_all.groupby('ts_code')['pst_amount_rank_10'].shift(2)
        ##加入前日open
        df_all['yesterday3_open']=df_all.groupby('ts_code')['open'].shift(3)
        df_all['yesterday3_high']=df_all.groupby('ts_code')['high'].shift(3)
        df_all['yesterday3_low']=df_all.groupby('ts_code')['low'].shift(3)
        df_all['yesterday3_pst_amount_rank']=df_all.groupby('ts_code')['pst_amount_rank_10'].shift(3)

        df_all.drop(['close','pre_close','pct_chg','pst_amount','adj_factor','real_price','amount'],axis=1,inplace=True)



        #暂时不用的列
        df_all=df_all[df_all['high_stop']==0]
        #'tomorrow_chg'
        df_all.drop(['high_stop'],axis=1,inplace=True)



        df_all.dropna(axis=0,how='any',inplace=True)


        month_sec=df_all['trade_date'].max()
        df_all=df_all[df_all['trade_date']==month_sec]
        print(df_all)
        df_all=df_all.reset_index(drop=True)

        df_all.to_csv('today_train.csv')
        dwdw=1

class FEg30_del2(FEbase):
    def __init__(self):
        pass

    def core(self,DataSetName):

        df_data=pd.read_csv(DataSetName[0],index_col=0,header=0)
        df_adj_all=pd.read_csv(DataSetName[1],index_col=0,header=0)
        #df_long_all=pd.read_csv(DataSetName[2],index_col=0,header=0)

        df_all=pd.merge(df_data, df_adj_all, how='inner', on=['ts_code','trade_date'])
        #df_all=pd.merge(df_all, df_long_all, how='inner', on=['ts_code','trade_date'])

        #===================================================================================================================================#
        #df_all['pe'] = df_all['pe'].fillna(9999)
        #df_all['pb'] = df_all['pb'].fillna(9999)

        #df_all['pe_rank']=df_all.groupby('trade_date')['pe'].rank(pct=True)
        #df_all['pb_rank']=df_all.groupby('trade_date')['pb'].rank(pct=True)        
        #df_all['pe_rank']=df_all['pe_rank']*10//1
        #df_all['pb_rank']=df_all['pb_rank']*10//1

        #df_all.drop(['turnover_rate','volume_ratio','pe','pb'],axis=1,inplace=True)

        #print(df_all)
        #df_all.to_csv('sjefosia.csv')

        #===================================================================================================================================#

        ##排除科创版
        #print(df_all)
        df_all=df_all[df_all['ts_code'].str.startswith('688')==False]
        

        ##加入gap_day特征
        #start=df_all['trade_date'].apply(str)[0]
        #end=df_all['trade_date'].apply(str)[df_all.shape[0]-1]
        #xxx=pd.date_range(start,end)

        #df = pd.DataFrame(xxx)
        #df.columns = ['trade_date']
        #df['trade_date']=df['trade_date'].map(str).map(lambda x : x[:4]+x[5:7]+x[8:10]).astype("int64")

        #yyy=df_all['trade_date']
        #zzz2=yyy.unique()
        #df_2=pd.DataFrame(zzz2)
        #df_2.columns = ['trade_date']
        #df_2['day_flag']=1
    
        #result = pd.merge(df, df_2, how='left', on=['trade_date'])
        #result['day_flag2']=result['day_flag'].shift(-1)
        #result['gap_day']=0

        #result.loc[(result['day_flag']==1) & (result['day_flag2']!=1),'gap_day']=1

        #result.drop(['day_flag','day_flag2'],axis=1,inplace=True)

        #df_all=pd.merge(df_all, result, how='left', on=['trade_date'])

        #===================================================================================================================================#

        #复权后价格
        df_all['real_price']=df_all['close']*df_all['adj_factor']
        
        #30日最低比值
        xxx=df_all.groupby('ts_code')['real_price'].rolling(30).min().reset_index()
        xxx.set_index(['level_1'], drop=True, append=False, inplace=True, verify_integrity=False)
        xxx.drop(['ts_code'],axis=1,inplace=True)
        
        df_all=df_all.join(xxx, lsuffix='', rsuffix='_30min')
        #bbb=df_all.groupby('ts_code')['real_price'].agg({'all_min':np.min, 'all_max': np.max}).reset_index()
        #ccc=pd.merge(df_all, bbb, how='inner', on=['ts_code'])
        df_all['30_pct']=(df_all['real_price']-df_all['real_price_30min'])/df_all['real_price_30min']
        df_all['30_pct_rank']=df_all.groupby('trade_date')['30_pct'].rank(pct=True)
        df_all['30_pct_rank']=df_all['30_pct_rank']*10//2

        #30日最高比值
        xxx=df_all.groupby('ts_code')['real_price'].rolling(30).max().reset_index()
        xxx.set_index(['level_1'], drop=True, append=False, inplace=True, verify_integrity=False)
        xxx.drop(['ts_code'],axis=1,inplace=True)
        
        df_all=df_all.join(xxx, lsuffix='', rsuffix='_30max')
        #bbb=df_all.groupby('ts_code')['real_price'].agg({'all_min':np.min, 'all_max': np.max}).reset_index()
        #ccc=pd.merge(df_all, bbb, how='inner', on=['ts_code'])
        df_all['30_pct_max']=(df_all['real_price']-df_all['real_price_30max'])/df_all['real_price_30max']
        df_all['30_pct_max_rank']=df_all.groupby('trade_date')['30_pct_max'].rank(pct=True)
        df_all['30_pct_max_rank']=df_all['30_pct_max_rank']*10//2



        df_all.drop(['30_pct','real_price_30min','30_pct_max','real_price_30max'],axis=1,inplace=True)
        #df_all.drop(['20_pct','real_price_20min','20_pct_max','real_price_20max'],axis=1,inplace=True)

        df_all.drop(['change','vol'],axis=1,inplace=True)

        #===================================================================================================================================#
    
        #明日幅度
        tm1=df_all.groupby('ts_code')['pct_chg'].shift(-1)
        tm2=df_all.groupby('ts_code')['pct_chg'].shift(-2)
        #df_all['tomorrow_chg']=((100+tm1)*(100+tm2)-10000)/100
        tm3=df_all.groupby('ts_code')['pct_chg'].shift(-3)
        tm4=df_all.groupby('ts_code')['pct_chg'].shift(-4)
        tm5=df_all.groupby('ts_code')['pct_chg'].shift(-5)

        df_all['tomorrow_chg']=(((100+tm1)/100)*((100+tm2)/100)*((100+tm3)/100)*((100+tm4)/100)*((100+tm5)/100)-1)*100

        #df_all['tomorrow_chg']=((100+tm1)*(100+tm2)*(100+tm3)-1000000)/10000
        #df_all['tomorrow_chg']=df_all.groupby('ts_code')['pct_chg'].shift(-1)
        #明日排名
        df_all['tomorrow_chg_rank']=df_all.groupby('trade_date')['tomorrow_chg'].rank(pct=True)
        df_all['tomorrow_chg_rank']=df_all['tomorrow_chg_rank']*9.9//1
        #是否停
        df_all['high_stop']=0
        df_all.loc[df_all['pct_chg']>9.4,'high_stop']=1
        df_all.loc[(df_all['pct_chg']<5.2) & (4.8<df_all['pct_chg']),'high_stop']=1


        ###真实价格范围(区分实际股价高低)
        #df_all['price_real_rank']=df_all.groupby('trade_date')['pre_close'].rank(pct=True)
        #df_all['price_real_rank']=df_all['price_real_rank']*10//1
        #1日
        df_all['chg_rank']=df_all.groupby('trade_date')['pct_chg'].rank(pct=True)
        df_all['chg_rank']=df_all['chg_rank']*10//2


        ##6日
        #xxx=df_all.groupby('ts_code')['chg_rank'].rolling(6).sum().reset_index()
        #xxx.set_index(['level_1'], drop=True, append=False, inplace=True, verify_integrity=False)
        #xxx.drop(['ts_code'],axis=1,inplace=True)

        #df_all=df_all.join(xxx, lsuffix='', rsuffix='_6')

        #df_all['chg_rank_6']=df_all.groupby('trade_date')['chg_rank_6'].rank(pct=True)
        #df_all['chg_rank_6']=df_all['chg_rank_6']*10//2

        #10日
        xxx=df_all.groupby('ts_code')['chg_rank'].rolling(10).sum().reset_index()
        xxx.set_index(['level_1'], drop=True, append=False, inplace=True, verify_integrity=False)
        xxx.drop(['ts_code'],axis=1,inplace=True)

        df_all=df_all.join(xxx, lsuffix='', rsuffix='_10')

        df_all['chg_rank_10']=df_all.groupby('trade_date')['chg_rank_10'].rank(pct=True)
        df_all['chg_rank_10']=df_all['chg_rank_10']*10//2

        #3日
        xxx=df_all.groupby('ts_code')['chg_rank'].rolling(3).sum().reset_index()
        xxx.set_index(['level_1'], drop=True, append=False, inplace=True, verify_integrity=False)
        xxx.drop(['ts_code'],axis=1,inplace=True)

        df_all=df_all.join(xxx, lsuffix='', rsuffix='_3')

        df_all['chg_rank_3']=df_all.groupby('trade_date')['chg_rank_3'].rank(pct=True)
        df_all['chg_rank_3']=df_all['chg_rank_3']*10//2

        ##20日
        #xxx=df_all.groupby('ts_code')['chg_rank'].rolling(20).sum().reset_index()
        #xxx.set_index(['level_1'], drop=True, append=False, inplace=True, verify_integrity=False)
        #xxx.drop(['ts_code'],axis=1,inplace=True)

        #df_all=df_all.join(xxx, lsuffix='', rsuffix='_20')

        #df_all['chg_rank_20']=df_all.groupby('trade_date')['chg_rank_3'].rank(pct=True)
        #df_all['chg_rank_20']=df_all['chg_rank_20']*10//1

        #print(df_all)

        #10日均量
        xxx=df_all.groupby('ts_code')['amount'].rolling(10).mean().reset_index()
        xxx.set_index(['level_1'], drop=True, append=False, inplace=True, verify_integrity=False)
        xxx.drop(['ts_code'],axis=1,inplace=True)
        df_all=df_all.join(xxx, lsuffix='', rsuffix='_10')

        #当日量占比
        df_all['pst_amount']=df_all['amount']/df_all['amount_10']
        df_all.drop(['amount_10'],axis=1,inplace=True)
        #当日量排名
        df_all['pst_amount_rank_10']=df_all.groupby('trade_date')['pst_amount'].rank(pct=True)
        df_all['pst_amount_rank_10']=df_all['pst_amount_rank_10']*10//2

        ##5日均量
        #xxx=df_all.groupby('ts_code')['amount'].rolling(5).mean().reset_index()
        #xxx.set_index(['level_1'], drop=True, append=False, inplace=True, verify_integrity=False)
        #xxx.drop(['ts_code'],axis=1,inplace=True)
        #df_all=df_all.join(xxx, lsuffix='', rsuffix='_5')

        ##当日量占比
        #df_all['pst_amount']=df_all['amount']/df_all['amount_5']
        #df_all.drop(['amount_5'],axis=1,inplace=True)
        ##当日量排名
        #df_all['pst_amount_rank_5']=df_all.groupby('trade_date')['pst_amount'].rank(pct=True)
        #df_all['pst_amount_rank_5']=df_all['pst_amount_rank_5']*10//1

        #计算三种比例rank
        dolist=['open','high','low']

        for curc in dolist:
            buffer=((df_all[curc]-df_all['pre_close'])*100)/df_all['close']
            df_all[curc]=buffer
            df_all[curc]=df_all.groupby('trade_date')[curc].rank(pct=True)
            df_all[curc]=df_all[curc]*10//2

        #加入昨日rank
        df_all['yesterday_open']=df_all.groupby('ts_code')['open'].shift(1)
        df_all['yesterday_high']=df_all.groupby('ts_code')['high'].shift(1)
        df_all['yesterday_low']=df_all.groupby('ts_code')['low'].shift(1)
        df_all['yesterday_pst_amount_rank']=df_all.groupby('ts_code')['pst_amount_rank_10'].shift(1)
        ##加入前日open
        df_all['yesterday2_open']=df_all.groupby('ts_code')['open'].shift(2)
        #df_all['yesterday2_high']=df_all.groupby('ts_code')['high'].shift(2)
        #df_all['yesterday2_low']=df_all.groupby('ts_code')['low'].shift(2)
        df_all['yesterday2_pst_amount_rank']=df_all.groupby('ts_code')['pst_amount_rank_10'].shift(2)
        ##加入前日open
        #df_all['yesterday3_open']=df_all.groupby('ts_code')['open'].shift(3)
        #df_all['yesterday3_high']=df_all.groupby('ts_code')['high'].shift(3)
        #df_all['yesterday3_low']=df_all.groupby('ts_code')['low'].shift(3)
        df_all['yesterday3_pst_amount_rank']=df_all.groupby('ts_code')['pst_amount_rank_10'].shift(3)


        df_all.drop(['close','pre_close','pct_chg','pst_amount','adj_factor','real_price','amount'],axis=1,inplace=True)
        #df_all.drop(['close','pre_close','pct_chg','adj_factor','real_price'],axis=1,inplace=True)

        #暂时不用的列
        df_all=df_all[df_all['high_stop']==0]

        #'tomorrow_chg'
        df_all.drop(['high_stop'],axis=1,inplace=True) 

        df_all.dropna(axis=0,how='any',inplace=True)

        print(df_all)
        df_all=df_all.reset_index(drop=True)

        return df_all

    def real_FE(self):

        df_data=pd.read_csv('real_now.csv',index_col=0,header=0)
        df_adj_all=pd.read_csv('real_adj_now.csv',index_col=0,header=0)

        df_all=pd.merge(df_data, df_adj_all, how='left', on=['ts_code','trade_date'])

        #df_all=df_all[df_all['ts_code'].str.startswith('688')==False]

        #df_all=pd.read_csv(bufferstring,index_col=0,header=0,nrows=100000)
    
        #df_all.drop(['change','vol'],axis=1,inplace=True)
 

        #===================================================================================================================================#

        #复权后价格
        df_all['adj_factor']=df_all['adj_factor'].fillna(0)
        df_all['real_price']=df_all['close']*df_all['adj_factor']
        
        df_all['real_price']=df_all.groupby('ts_code')['real_price'].shift(1)
        df_all['real_price']=df_all['real_price']*(1+df_all['pct_chg']/100)


        #30日最低比值
        xxx=df_all.groupby('ts_code')['real_price'].rolling(30).min().reset_index()
        xxx.set_index(['level_1'], drop=True, append=False, inplace=True, verify_integrity=False)
        xxx.drop(['ts_code'],axis=1,inplace=True)
        
        df_all=df_all.join(xxx, lsuffix='', rsuffix='_30min')
        #bbb=df_all.groupby('ts_code')['real_price'].agg({'all_min':np.min, 'all_max': np.max}).reset_index()
        #ccc=pd.merge(df_all, bbb, how='inner', on=['ts_code'])
        df_all['30_pct']=(df_all['real_price']-df_all['real_price_30min'])/df_all['real_price_30min']
        df_all['30_pct_rank']=df_all.groupby('trade_date')['30_pct'].rank(pct=True)
        df_all['30_pct_rank']=df_all['30_pct_rank']*10//2

        #30日最高比值
        xxx=df_all.groupby('ts_code')['real_price'].rolling(30).max().reset_index()
        xxx.set_index(['level_1'], drop=True, append=False, inplace=True, verify_integrity=False)
        xxx.drop(['ts_code'],axis=1,inplace=True)
        
        df_all=df_all.join(xxx, lsuffix='', rsuffix='_30max')
        #bbb=df_all.groupby('ts_code')['real_price'].agg({'all_min':np.min, 'all_max': np.max}).reset_index()
        #ccc=pd.merge(df_all, bbb, how='inner', on=['ts_code'])
        df_all['30_pct_max']=(df_all['real_price']-df_all['real_price_30max'])/df_all['real_price_30max']
        df_all['30_pct_max_rank']=df_all.groupby('trade_date')['30_pct_max'].rank(pct=True)
        df_all['30_pct_max_rank']=df_all['30_pct_max_rank']*10//2

        df_all.drop(['30_pct','real_price_30min','30_pct_max','real_price_30max'],axis=1,inplace=True)


        #===================================================================================================================================#


        #是否停
        df_all['high_stop']=0
        df_all.loc[df_all['pct_chg']>9.7,'high_stop']=1
        df_all.loc[(df_all['pct_chg']<5.2) & (4.8<df_all['pct_chg']),'high_stop']=1


        #1日
        df_all['chg_rank']=df_all.groupby('trade_date')['pct_chg'].rank(pct=True)
        df_all['chg_rank']=df_all['chg_rank']*10//2


        #6日
        xxx=df_all.groupby('ts_code')['chg_rank'].rolling(6).sum().reset_index()
        xxx.set_index(['level_1'], drop=True, append=False, inplace=True, verify_integrity=False)
        xxx.drop(['ts_code'],axis=1,inplace=True)

        df_all=df_all.join(xxx, lsuffix='', rsuffix='_6')

        df_all['chg_rank_6']=df_all.groupby('trade_date')['chg_rank_6'].rank(pct=True)
        df_all['chg_rank_6']=df_all['chg_rank_6']*10//2

        #10日
        xxx=df_all.groupby('ts_code')['chg_rank'].rolling(10).sum().reset_index()
        xxx.set_index(['level_1'], drop=True, append=False, inplace=True, verify_integrity=False)
        xxx.drop(['ts_code'],axis=1,inplace=True)

        df_all=df_all.join(xxx, lsuffix='', rsuffix='_10')

        df_all['chg_rank_10']=df_all.groupby('trade_date')['chg_rank_10'].rank(pct=True)
        df_all['chg_rank_10']=df_all['chg_rank_10']*10//2

        #3日
        xxx=df_all.groupby('ts_code')['chg_rank'].rolling(3).sum().reset_index()
        xxx.set_index(['level_1'], drop=True, append=False, inplace=True, verify_integrity=False)
        xxx.drop(['ts_code'],axis=1,inplace=True)

        df_all=df_all.join(xxx, lsuffix='', rsuffix='_3')

        df_all['chg_rank_3']=df_all.groupby('trade_date')['chg_rank_3'].rank(pct=True)
        df_all['chg_rank_3']=df_all['chg_rank_3']*10//2

        #print(df_all)

        #10日均量
        xxx=df_all.groupby('ts_code')['amount'].rolling(10).mean().reset_index()
        xxx.set_index(['level_1'], drop=True, append=False, inplace=True, verify_integrity=False)
        xxx.drop(['ts_code'],axis=1,inplace=True)
        df_all=df_all.join(xxx, lsuffix='', rsuffix='_10')

        #当日量占比
        df_all['pst_amount']=df_all['amount']/df_all['amount_10']
        df_all.drop(['amount_10'],axis=1,inplace=True)
        #当日量排名
        df_all['pst_amount_rank_10']=df_all.groupby('trade_date')['pst_amount'].rank(pct=True)
        df_all['pst_amount_rank_10']=df_all['pst_amount_rank_10']*10//2

        #计算三种比例rank
        dolist=['open','high','low']

        for curc in dolist:
            buffer=((df_all[curc]-df_all['pre_close'])*100)/df_all['pre_close']
            df_all[curc]=buffer
            df_all[curc]=df_all.groupby('trade_date')[curc].rank(pct=True)
            df_all[curc]=df_all[curc]*10//2

        #加入昨日rank
        df_all['yesterday_open']=df_all.groupby('ts_code')['open'].shift(1)
        df_all['yesterday_high']=df_all.groupby('ts_code')['high'].shift(1)
        df_all['yesterday_low']=df_all.groupby('ts_code')['low'].shift(1)
        df_all['yesterday_pst_amount_rank']=df_all.groupby('ts_code')['pst_amount_rank_10'].shift(1)
        ##加入前日open
        df_all['yesterday2_open']=df_all.groupby('ts_code')['open'].shift(2)
        df_all['yesterday2_high']=df_all.groupby('ts_code')['high'].shift(2)
        df_all['yesterday2_low']=df_all.groupby('ts_code')['low'].shift(2)
        df_all['yesterday2_pst_amount_rank']=df_all.groupby('ts_code')['pst_amount_rank_10'].shift(2)
        ##加入前日open
        df_all['yesterday3_open']=df_all.groupby('ts_code')['open'].shift(3)
        df_all['yesterday3_high']=df_all.groupby('ts_code')['high'].shift(3)
        df_all['yesterday3_low']=df_all.groupby('ts_code')['low'].shift(3)
        df_all['yesterday3_pst_amount_rank']=df_all.groupby('ts_code')['pst_amount_rank_10'].shift(3)

        df_all.drop(['close','pre_close','pct_chg','pst_amount','adj_factor','real_price','amount'],axis=1,inplace=True)



        #暂时不用的列
        df_all=df_all[df_all['high_stop']==0]
        #'tomorrow_chg'
        df_all.drop(['high_stop'],axis=1,inplace=True)



        df_all.dropna(axis=0,how='any',inplace=True)


        month_sec=df_all['trade_date'].max()
        df_all=df_all[df_all['trade_date']==month_sec]
        print(df_all)
        df_all=df_all.reset_index(drop=True)

        df_all.to_csv('today_train.csv')
        dwdw=1

class FEg20next3(FEbase):
    def __init__(self):
        pass

    def core(self,DataSetName):

        df_data=pd.read_csv(DataSetName[0],index_col=0,header=0)
        df_adj_all=pd.read_csv(DataSetName[1],index_col=0,header=0)
        #df_long_all=pd.read_csv(DataSetName[2],index_col=0,header=0)

        df_all=pd.merge(df_data, df_adj_all, how='inner', on=['ts_code','trade_date'])
        #df_all=pd.merge(df_all, df_long_all, how='inner', on=['ts_code','trade_date'])

        #===================================================================================================================================#
        #df_all['pe'] = df_all['pe'].fillna(9999)
        #df_all['pb'] = df_all['pb'].fillna(9999)

        #df_all['pe_rank']=df_all.groupby('trade_date')['pe'].rank(pct=True)
        #df_all['pb_rank']=df_all.groupby('trade_date')['pb'].rank(pct=True)        
        #df_all['pe_rank']=df_all['pe_rank']*10//1
        #df_all['pb_rank']=df_all['pb_rank']*10//1

        #df_all.drop(['turnover_rate','volume_ratio','pe','pb'],axis=1,inplace=True)

        #print(df_all)
        #df_all.to_csv('sjefosia.csv')

        #===================================================================================================================================#

        ##排除科创版
        #print(df_all)
        df_all=df_all[df_all['ts_code'].str.startswith('688')==False]
        

        ##加入gap_day特征
        #start=df_all['trade_date'].apply(str)[0]
        #end=df_all['trade_date'].apply(str)[df_all.shape[0]-1]
        #xxx=pd.date_range(start,end)

        #df = pd.DataFrame(xxx)
        #df.columns = ['trade_date']
        #df['trade_date']=df['trade_date'].map(str).map(lambda x : x[:4]+x[5:7]+x[8:10]).astype("int64")

        #yyy=df_all['trade_date']
        #zzz2=yyy.unique()
        #df_2=pd.DataFrame(zzz2)
        #df_2.columns = ['trade_date']
        #df_2['day_flag']=1
    
        #result = pd.merge(df, df_2, how='left', on=['trade_date'])
        #result['day_flag2']=result['day_flag'].shift(-1)
        #result['gap_day']=0

        #result.loc[(result['day_flag']==1) & (result['day_flag2']!=1),'gap_day']=1

        #result.drop(['day_flag','day_flag2'],axis=1,inplace=True)

        #df_all=pd.merge(df_all, result, how='left', on=['trade_date'])

        #===================================================================================================================================#

        #复权后价格
        df_all['real_price']=df_all['close']*df_all['adj_factor']
        
        ##30日最低比值
        #xxx=df_all.groupby('ts_code')['real_price'].rolling(30).min().reset_index()
        #xxx.set_index(['level_1'], drop=True, append=False, inplace=True, verify_integrity=False)
        #xxx.drop(['ts_code'],axis=1,inplace=True)
        
        #df_all=df_all.join(xxx, lsuffix='', rsuffix='_30min')
        ##bbb=df_all.groupby('ts_code')['real_price'].agg({'all_min':np.min, 'all_max': np.max}).reset_index()
        ##ccc=pd.merge(df_all, bbb, how='inner', on=['ts_code'])
        #df_all['30_pct']=(df_all['real_price']-df_all['real_price_30min'])/df_all['real_price_30min']
        #df_all['30_pct_rank']=df_all.groupby('trade_date')['30_pct'].rank(pct=True)
        #df_all['30_pct_rank']=df_all['30_pct_rank']*10//2

        ##30日最高比值
        #xxx=df_all.groupby('ts_code')['real_price'].rolling(30).max().reset_index()
        #xxx.set_index(['level_1'], drop=True, append=False, inplace=True, verify_integrity=False)
        #xxx.drop(['ts_code'],axis=1,inplace=True)
        
        #df_all=df_all.join(xxx, lsuffix='', rsuffix='_30max')
        ##bbb=df_all.groupby('ts_code')['real_price'].agg({'all_min':np.min, 'all_max': np.max}).reset_index()
        ##ccc=pd.merge(df_all, bbb, how='inner', on=['ts_code'])
        #df_all['30_pct_max']=(df_all['real_price']-df_all['real_price_30max'])/df_all['real_price_30max']
        #df_all['30_pct_max_rank']=df_all.groupby('trade_date')['30_pct_max'].rank(pct=True)
        #df_all['30_pct_max_rank']=df_all['30_pct_max_rank']*10//2


        #60日最低比值
        xxx=df_all.groupby('ts_code')['real_price'].rolling(20).min().reset_index()
        xxx.set_index(['level_1'], drop=True, append=False, inplace=True, verify_integrity=False)
        xxx.drop(['ts_code'],axis=1,inplace=True)
        
        df_all=df_all.join(xxx, lsuffix='', rsuffix='_20min')
        #bbb=df_all.groupby('ts_code')['real_price'].agg({'all_min':np.min, 'all_max': np.max}).reset_index()
        #ccc=pd.merge(df_all, bbb, how='inner', on=['ts_code'])
        df_all['20_pct']=(df_all['real_price']-df_all['real_price_20min'])/df_all['real_price_20min']
        df_all['20_pct_rank']=df_all.groupby('trade_date')['20_pct'].rank(pct=True)
        df_all['20_pct_rank']=df_all['20_pct_rank']*10//2

        #20日最高比值
        xxx=df_all.groupby('ts_code')['real_price'].rolling(20).max().reset_index()
        xxx.set_index(['level_1'], drop=True, append=False, inplace=True, verify_integrity=False)
        xxx.drop(['ts_code'],axis=1,inplace=True)
        
        df_all=df_all.join(xxx, lsuffix='', rsuffix='_20max')
        #bbb=df_all.groupby('ts_code')['real_price'].agg({'all_min':np.min, 'all_max': np.max}).reset_index()
        #ccc=pd.merge(df_all, bbb, how='inner', on=['ts_code'])
        df_all['20_pct_max']=(df_all['real_price']-df_all['real_price_20max'])/df_all['real_price_20max']
        df_all['20_pct_max_rank']=df_all.groupby('trade_date')['20_pct_max'].rank(pct=True)
        df_all['20_pct_max_rank']=df_all['20_pct_max_rank']*10//2


        #df_all.drop(['30_pct','real_price_30min','30_pct_max','real_price_30max'],axis=1,inplace=True)
        df_all.drop(['20_pct','real_price_20min','20_pct_max','real_price_20max'],axis=1,inplace=True)

        df_all.drop(['change','vol'],axis=1,inplace=True)

        #===================================================================================================================================#
    
        #明日幅度
        tm1=df_all.groupby('ts_code')['pct_chg'].shift(-1)
        tm2=df_all.groupby('ts_code')['pct_chg'].shift(-2)
        #df_all['tomorrow_chg']=((100+tm1)*(100+tm2)-10000)/100
        tm3=df_all.groupby('ts_code')['pct_chg'].shift(-3)
        #tm4=df_all.groupby('ts_code')['pct_chg'].shift(-4)
        #tm5=df_all.groupby('ts_code')['pct_chg'].shift(-5)
        #tm6=df_all.groupby('ts_code')['pct_chg'].shift(-6)
        #tm7=df_all.groupby('ts_code')['pct_chg'].shift(-7)
        #tm8=df_all.groupby('ts_code')['pct_chg'].shift(-8)
        #tm9=df_all.groupby('ts_code')['pct_chg'].shift(-9)
        #tm10=df_all.groupby('ts_code')['pct_chg'].shift(-10)


        #df_all['tomorrow_chg']=(((100+tm1)/100)*((100+tm2)/100)*((100+tm3)/100)*((100+tm4)/100)*((100+tm5)/100)*((100+tm6)/100)*((100+tm7)/100)*((100+tm8)/100)*((100+tm9)/100)*((100+tm10)/100)-1)*100
        df_all['tomorrow_chg']=(((100+tm1)/100)*((100+tm2)/100)*((100+tm3)/100)-1)*100



        #df_all['tomorrow_chg']=((100+tm1)*(100+tm2)*(100+tm3)-1000000)/10000
        #df_all['tomorrow_chg']=df_all.groupby('ts_code')['pct_chg'].shift(-1)
        #明日排名
        df_all['tomorrow_chg_rank']=df_all.groupby('trade_date')['tomorrow_chg'].rank(pct=True)
        df_all['tomorrow_chg_rank']=df_all['tomorrow_chg_rank']*9.9//1
        #是否停
        df_all['high_stop']=0
        df_all.loc[df_all['pct_chg']>9.4,'high_stop']=1
        df_all.loc[(df_all['pct_chg']<5.2) & (4.8<df_all['pct_chg']),'high_stop']=1


        ###真实价格范围(区分实际股价高低)
        #df_all['price_real_rank']=df_all.groupby('trade_date')['pre_close'].rank(pct=True)
        #df_all['price_real_rank']=df_all['price_real_rank']*10//1
        #1日
        df_all['chg_rank']=df_all.groupby('trade_date')['pct_chg'].rank(pct=True)
        df_all['chg_rank']=df_all['chg_rank']*10//2


        #6日
        xxx=df_all.groupby('ts_code')['chg_rank'].rolling(6).sum().reset_index()
        xxx.set_index(['level_1'], drop=True, append=False, inplace=True, verify_integrity=False)
        xxx.drop(['ts_code'],axis=1,inplace=True)

        df_all=df_all.join(xxx, lsuffix='', rsuffix='_6')

        df_all['chg_rank_6']=df_all.groupby('trade_date')['chg_rank_6'].rank(pct=True)
        df_all['chg_rank_6']=df_all['chg_rank_6']*10//2

        #10日
        xxx=df_all.groupby('ts_code')['chg_rank'].rolling(10).sum().reset_index()
        xxx.set_index(['level_1'], drop=True, append=False, inplace=True, verify_integrity=False)
        xxx.drop(['ts_code'],axis=1,inplace=True)

        df_all=df_all.join(xxx, lsuffix='', rsuffix='_10')

        df_all['chg_rank_10']=df_all.groupby('trade_date')['chg_rank_10'].rank(pct=True)
        df_all['chg_rank_10']=df_all['chg_rank_10']*10//2

        #3日
        xxx=df_all.groupby('ts_code')['chg_rank'].rolling(3).sum().reset_index()
        xxx.set_index(['level_1'], drop=True, append=False, inplace=True, verify_integrity=False)
        xxx.drop(['ts_code'],axis=1,inplace=True)

        df_all=df_all.join(xxx, lsuffix='', rsuffix='_3')

        df_all['chg_rank_3']=df_all.groupby('trade_date')['chg_rank_3'].rank(pct=True)
        df_all['chg_rank_3']=df_all['chg_rank_3']*10//2

        ##20日
        #xxx=df_all.groupby('ts_code')['chg_rank'].rolling(20).sum().reset_index()
        #xxx.set_index(['level_1'], drop=True, append=False, inplace=True, verify_integrity=False)
        #xxx.drop(['ts_code'],axis=1,inplace=True)

        #df_all=df_all.join(xxx, lsuffix='', rsuffix='_20')

        #df_all['chg_rank_20']=df_all.groupby('trade_date')['chg_rank_3'].rank(pct=True)
        #df_all['chg_rank_20']=df_all['chg_rank_20']*10//1

        #print(df_all)

        #10日均量
        xxx=df_all.groupby('ts_code')['amount'].rolling(10).mean().reset_index()
        xxx.set_index(['level_1'], drop=True, append=False, inplace=True, verify_integrity=False)
        xxx.drop(['ts_code'],axis=1,inplace=True)
        df_all=df_all.join(xxx, lsuffix='', rsuffix='_10')

        #当日量占比
        df_all['pst_amount']=df_all['amount']/df_all['amount_10']
        df_all.drop(['amount_10'],axis=1,inplace=True)
        #当日量排名
        df_all['pst_amount_rank_10']=df_all.groupby('trade_date')['pst_amount'].rank(pct=True)
        df_all['pst_amount_rank_10']=df_all['pst_amount_rank_10']*10//2

        ##5日均量
        #xxx=df_all.groupby('ts_code')['amount'].rolling(5).mean().reset_index()
        #xxx.set_index(['level_1'], drop=True, append=False, inplace=True, verify_integrity=False)
        #xxx.drop(['ts_code'],axis=1,inplace=True)
        #df_all=df_all.join(xxx, lsuffix='', rsuffix='_5')

        ##当日量占比
        #df_all['pst_amount']=df_all['amount']/df_all['amount_5']
        #df_all.drop(['amount_5'],axis=1,inplace=True)
        ##当日量排名
        #df_all['pst_amount_rank_5']=df_all.groupby('trade_date')['pst_amount'].rank(pct=True)
        #df_all['pst_amount_rank_5']=df_all['pst_amount_rank_5']*10//1

        #计算三种比例rank
        dolist=['open','high','low']

        for curc in dolist:
            buffer=((df_all[curc]-df_all['pre_close'])*100)/df_all['close']
            df_all[curc]=buffer
            df_all[curc]=df_all.groupby('trade_date')[curc].rank(pct=True)
            df_all[curc]=df_all[curc]*10//2

        #加入昨日rank
        df_all['yesterday_open']=df_all.groupby('ts_code')['open'].shift(1)
        df_all['yesterday_high']=df_all.groupby('ts_code')['high'].shift(1)
        df_all['yesterday_low']=df_all.groupby('ts_code')['low'].shift(1)
        df_all['yesterday_pst_amount_rank']=df_all.groupby('ts_code')['pst_amount_rank_10'].shift(1)
        ##加入前日open
        df_all['yesterday2_open']=df_all.groupby('ts_code')['open'].shift(2)
        df_all['yesterday2_high']=df_all.groupby('ts_code')['high'].shift(2)
        df_all['yesterday2_low']=df_all.groupby('ts_code')['low'].shift(2)
        df_all['yesterday2_pst_amount_rank']=df_all.groupby('ts_code')['pst_amount_rank_10'].shift(2)
        ##加入前日open
        df_all['yesterday3_open']=df_all.groupby('ts_code')['open'].shift(3)
        df_all['yesterday3_high']=df_all.groupby('ts_code')['high'].shift(3)
        df_all['yesterday3_low']=df_all.groupby('ts_code')['low'].shift(3)
        df_all['yesterday3_pst_amount_rank']=df_all.groupby('ts_code')['pst_amount_rank_10'].shift(3)


        df_all.drop(['close','pre_close','pct_chg','pst_amount','adj_factor','real_price','amount'],axis=1,inplace=True)
        #df_all.drop(['close','pre_close','pct_chg','adj_factor','real_price'],axis=1,inplace=True)

        #暂时不用的列
        df_all=df_all[df_all['high_stop']==0]

        #'tomorrow_chg'
        df_all.drop(['high_stop'],axis=1,inplace=True) 

        df_all.dropna(axis=0,how='any',inplace=True)

        print(df_all)
        df_all=df_all.reset_index(drop=True)

        return df_all

    def real_FE(self):

        df_data=pd.read_csv('real_now.csv',index_col=0,header=0)
        df_adj_all=pd.read_csv('real_adj_now.csv',index_col=0,header=0)

        df_all=pd.merge(df_data, df_adj_all, how='left', on=['ts_code','trade_date'])

        #df_all=df_all[df_all['ts_code'].str.startswith('688')==False]

        #df_all=pd.read_csv(bufferstring,index_col=0,header=0,nrows=100000)
    
        #df_all.drop(['change','vol'],axis=1,inplace=True)
 

        #===================================================================================================================================#

        #复权后价格
        df_all['adj_factor']=df_all['adj_factor'].fillna(0)
        df_all['real_price']=df_all['close']*df_all['adj_factor']
        
        df_all['real_price']=df_all.groupby('ts_code')['real_price'].shift(1)
        df_all['real_price']=df_all['real_price']*(1+df_all['pct_chg']/100)


        #30日最低比值
        xxx=df_all.groupby('ts_code')['real_price'].rolling(30).min().reset_index()
        xxx.set_index(['level_1'], drop=True, append=False, inplace=True, verify_integrity=False)
        xxx.drop(['ts_code'],axis=1,inplace=True)
        
        df_all=df_all.join(xxx, lsuffix='', rsuffix='_30min')
        #bbb=df_all.groupby('ts_code')['real_price'].agg({'all_min':np.min, 'all_max': np.max}).reset_index()
        #ccc=pd.merge(df_all, bbb, how='inner', on=['ts_code'])
        df_all['30_pct']=(df_all['real_price']-df_all['real_price_30min'])/df_all['real_price_30min']
        df_all['30_pct_rank']=df_all.groupby('trade_date')['30_pct'].rank(pct=True)
        df_all['30_pct_rank']=df_all['30_pct_rank']*10//2

        #30日最高比值
        xxx=df_all.groupby('ts_code')['real_price'].rolling(30).max().reset_index()
        xxx.set_index(['level_1'], drop=True, append=False, inplace=True, verify_integrity=False)
        xxx.drop(['ts_code'],axis=1,inplace=True)
        
        df_all=df_all.join(xxx, lsuffix='', rsuffix='_30max')
        #bbb=df_all.groupby('ts_code')['real_price'].agg({'all_min':np.min, 'all_max': np.max}).reset_index()
        #ccc=pd.merge(df_all, bbb, how='inner', on=['ts_code'])
        df_all['30_pct_max']=(df_all['real_price']-df_all['real_price_30max'])/df_all['real_price_30max']
        df_all['30_pct_max_rank']=df_all.groupby('trade_date')['30_pct_max'].rank(pct=True)
        df_all['30_pct_max_rank']=df_all['30_pct_max_rank']*10//2

        df_all.drop(['30_pct','real_price_30min','30_pct_max','real_price_30max'],axis=1,inplace=True)


        #===================================================================================================================================#


        #是否停
        df_all['high_stop']=0
        df_all.loc[df_all['pct_chg']>9.7,'high_stop']=1
        df_all.loc[(df_all['pct_chg']<5.2) & (4.8<df_all['pct_chg']),'high_stop']=1


        #1日
        df_all['chg_rank']=df_all.groupby('trade_date')['pct_chg'].rank(pct=True)
        df_all['chg_rank']=df_all['chg_rank']*10//2


        #6日
        xxx=df_all.groupby('ts_code')['chg_rank'].rolling(6).sum().reset_index()
        xxx.set_index(['level_1'], drop=True, append=False, inplace=True, verify_integrity=False)
        xxx.drop(['ts_code'],axis=1,inplace=True)

        df_all=df_all.join(xxx, lsuffix='', rsuffix='_6')

        df_all['chg_rank_6']=df_all.groupby('trade_date')['chg_rank_6'].rank(pct=True)
        df_all['chg_rank_6']=df_all['chg_rank_6']*10//2

        #10日
        xxx=df_all.groupby('ts_code')['chg_rank'].rolling(10).sum().reset_index()
        xxx.set_index(['level_1'], drop=True, append=False, inplace=True, verify_integrity=False)
        xxx.drop(['ts_code'],axis=1,inplace=True)

        df_all=df_all.join(xxx, lsuffix='', rsuffix='_10')

        df_all['chg_rank_10']=df_all.groupby('trade_date')['chg_rank_10'].rank(pct=True)
        df_all['chg_rank_10']=df_all['chg_rank_10']*10//2

        #3日
        xxx=df_all.groupby('ts_code')['chg_rank'].rolling(3).sum().reset_index()
        xxx.set_index(['level_1'], drop=True, append=False, inplace=True, verify_integrity=False)
        xxx.drop(['ts_code'],axis=1,inplace=True)

        df_all=df_all.join(xxx, lsuffix='', rsuffix='_3')

        df_all['chg_rank_3']=df_all.groupby('trade_date')['chg_rank_3'].rank(pct=True)
        df_all['chg_rank_3']=df_all['chg_rank_3']*10//2

        #print(df_all)

        #10日均量
        xxx=df_all.groupby('ts_code')['amount'].rolling(10).mean().reset_index()
        xxx.set_index(['level_1'], drop=True, append=False, inplace=True, verify_integrity=False)
        xxx.drop(['ts_code'],axis=1,inplace=True)
        df_all=df_all.join(xxx, lsuffix='', rsuffix='_10')

        #当日量占比
        df_all['pst_amount']=df_all['amount']/df_all['amount_10']
        df_all.drop(['amount_10'],axis=1,inplace=True)
        #当日量排名
        df_all['pst_amount_rank_10']=df_all.groupby('trade_date')['pst_amount'].rank(pct=True)
        df_all['pst_amount_rank_10']=df_all['pst_amount_rank_10']*10//2

        #计算三种比例rank
        dolist=['open','high','low']

        for curc in dolist:
            buffer=((df_all[curc]-df_all['pre_close'])*100)/df_all['pre_close']
            df_all[curc]=buffer
            df_all[curc]=df_all.groupby('trade_date')[curc].rank(pct=True)
            df_all[curc]=df_all[curc]*10//2

        #加入昨日rank
        df_all['yesterday_open']=df_all.groupby('ts_code')['open'].shift(1)
        df_all['yesterday_high']=df_all.groupby('ts_code')['high'].shift(1)
        df_all['yesterday_low']=df_all.groupby('ts_code')['low'].shift(1)
        df_all['yesterday_pst_amount_rank']=df_all.groupby('ts_code')['pst_amount_rank_10'].shift(1)
        ##加入前日open
        df_all['yesterday2_open']=df_all.groupby('ts_code')['open'].shift(2)
        df_all['yesterday2_high']=df_all.groupby('ts_code')['high'].shift(2)
        df_all['yesterday2_low']=df_all.groupby('ts_code')['low'].shift(2)
        df_all['yesterday2_pst_amount_rank']=df_all.groupby('ts_code')['pst_amount_rank_10'].shift(2)
        ##加入前日open
        df_all['yesterday3_open']=df_all.groupby('ts_code')['open'].shift(3)
        df_all['yesterday3_high']=df_all.groupby('ts_code')['high'].shift(3)
        df_all['yesterday3_low']=df_all.groupby('ts_code')['low'].shift(3)
        df_all['yesterday3_pst_amount_rank']=df_all.groupby('ts_code')['pst_amount_rank_10'].shift(3)

        df_all.drop(['close','pre_close','pct_chg','pst_amount','adj_factor','real_price','amount'],axis=1,inplace=True)



        #暂时不用的列
        df_all=df_all[df_all['high_stop']==0]
        #'tomorrow_chg'
        df_all.drop(['high_stop'],axis=1,inplace=True)



        df_all.dropna(axis=0,how='any',inplace=True)


        month_sec=df_all['trade_date'].max()
        df_all=df_all[df_all['trade_date']!=month_sec]
        print(df_all)
        df_all=df_all.reset_index(drop=True)

        df_all.to_csv('today_train.csv')
        dwdw=1

class FEd(FEbase):
    def __init__(self):
        pass

    def core(self,DataSetName):

        df_data=pd.read_csv(DataSetName[0],index_col=0,header=0)
        df_adj_all=pd.read_csv(DataSetName[1],index_col=0,header=0)
        #df_long_all=pd.read_csv(DataSetName[2],index_col=0,header=0)

        df_all=pd.merge(df_data, df_adj_all, how='inner', on=['ts_code','trade_date'])
        #df_all=pd.merge(df_all, df_long_all, how='inner', on=['ts_code','trade_date'])

        #===================================================================================================================================#
        #df_all['pe'] = df_all['pe'].fillna(9999)
        #df_all['pb'] = df_all['pb'].fillna(9999)

        #df_all['pe_rank']=df_all.groupby('trade_date')['pe'].rank(pct=True)
        #df_all['pb_rank']=df_all.groupby('trade_date')['pb'].rank(pct=True)        
        #df_all['pe_rank']=df_all['pe_rank']*10//1
        #df_all['pb_rank']=df_all['pb_rank']*10//1

        #df_all.drop(['turnover_rate','volume_ratio','pe','pb'],axis=1,inplace=True)

        #print(df_all)
        #df_all.to_csv('sjefosia.csv')

        #===================================================================================================================================#

        ##排除科创版
        #print(df_all)
        df_all=df_all[df_all['ts_code'].str.startswith('688')==False]
        

        #加入gap_day特征
        start=df_all['trade_date'].apply(str)[0]
        end=df_all['trade_date'].apply(str)[df_all.shape[0]-1]
        xxx=pd.date_range(start,end)

        df = pd.DataFrame(xxx)
        df.columns = ['trade_date']
        df['trade_date']=df['trade_date'].map(str).map(lambda x : x[:4]+x[5:7]+x[8:10]).astype("int64")

        yyy=df_all['trade_date']
        zzz2=yyy.unique()
        df_2=pd.DataFrame(zzz2)
        df_2.columns = ['trade_date']
        df_2['day_flag']=1
    
        result = pd.merge(df, df_2, how='left', on=['trade_date'])
        result['day_flag2']=result['day_flag'].shift(-1)
        result['gap_day']=0

        result.loc[(result['day_flag']==1) & (result['day_flag2']!=1),'gap_day']=1

        result.drop(['day_flag','day_flag2'],axis=1,inplace=True)

        df_all=pd.merge(df_all, result, how='left', on=['trade_date'])

        #===================================================================================================================================#

        #复权后价格
        df_all['real_price']=df_all['close']*df_all['adj_factor']
        
        #30日最低比值
        xxx=df_all.groupby('ts_code')['real_price'].rolling(30).min().reset_index()
        xxx.set_index(['level_1'], drop=True, append=False, inplace=True, verify_integrity=False)
        xxx.drop(['ts_code'],axis=1,inplace=True)
        
        df_all=df_all.join(xxx, lsuffix='', rsuffix='_30min')
        #bbb=df_all.groupby('ts_code')['real_price'].agg({'all_min':np.min, 'all_max': np.max}).reset_index()
        #ccc=pd.merge(df_all, bbb, how='inner', on=['ts_code'])
        df_all['30_pct']=(df_all['real_price']-df_all['real_price_30min'])/df_all['real_price_30min']
        df_all['30_pct_rank']=df_all.groupby('trade_date')['30_pct'].rank(pct=True)
        df_all['30_pct_rank']=df_all['30_pct_rank']*10//2

        #30日最高比值
        xxx=df_all.groupby('ts_code')['real_price'].rolling(30).max().reset_index()
        xxx.set_index(['level_1'], drop=True, append=False, inplace=True, verify_integrity=False)
        xxx.drop(['ts_code'],axis=1,inplace=True)
        
        df_all=df_all.join(xxx, lsuffix='', rsuffix='_30max')
        #bbb=df_all.groupby('ts_code')['real_price'].agg({'all_min':np.min, 'all_max': np.max}).reset_index()
        #ccc=pd.merge(df_all, bbb, how='inner', on=['ts_code'])
        df_all['30_pct_max']=(df_all['real_price']-df_all['real_price_30max'])/df_all['real_price_30max']
        df_all['30_pct_max_rank']=df_all.groupby('trade_date')['30_pct_max'].rank(pct=True)
        df_all['30_pct_max_rank']=df_all['30_pct_max_rank']*10//2

        df_all.drop(['30_pct','real_price_30min','30_pct_max','real_price_30max'],axis=1,inplace=True)

        df_all.drop(['change','vol'],axis=1,inplace=True)

        #===================================================================================================================================#
    
        #明日幅度
        tm1=df_all.groupby('ts_code')['pct_chg'].shift(-1)
        tm2=df_all.groupby('ts_code')['pct_chg'].shift(-2)
        #df_all['tomorrow_chg']=((100+tm1)*(100+tm2)-10000)/100
        tm3=df_all.groupby('ts_code')['pct_chg'].shift(-3)
        tm4=df_all.groupby('ts_code')['pct_chg'].shift(-4)
        tm5=df_all.groupby('ts_code')['pct_chg'].shift(-5)

        df_all['tomorrow_chg']=(((100+tm1)/100)*((100+tm2)/100)*((100+tm3)/100)*((100+tm4)/100)*((100+tm5)/100)-1)*100

        #df_all['tomorrow_chg']=((100+tm1)*(100+tm2)*(100+tm3)-1000000)/10000
        #df_all['tomorrow_chg']=df_all.groupby('ts_code')['pct_chg'].shift(-1)
        #明日排名
        df_all['tomorrow_chg_rank']=df_all.groupby('trade_date')['tomorrow_chg'].rank(pct=True)
        df_all['tomorrow_chg_rank']=df_all['tomorrow_chg_rank']*9.9//1
        #是否停
        df_all['high_stop']=0
        df_all.loc[df_all['pct_chg']>9.4,'high_stop']=1
        df_all.loc[(df_all['pct_chg']<5.2) & (4.8<df_all['pct_chg']),'high_stop']=1


        ##真实价格范围(区分实际股价高低)
        #df_all['price_real_rank']=df_all.groupby('trade_date')['pre_close'].rank(pct=True)
        #df_all['price_real_rank']=df_all['price_real_rank']*10//1
        #1日
        df_all['chg_rank']=df_all.groupby('trade_date')['pct_chg'].rank(pct=True)
        df_all['chg_rank']=df_all['chg_rank']*10//2

        df_all['y_1_chg']=df_all.groupby('ts_code')['pct_chg'].shift(1)
        df_all['y_2_chg']=df_all.groupby('ts_code')['pct_chg'].shift(2)
        df_all['y_3_chg']=df_all.groupby('ts_code')['pct_chg'].shift(3)
        df_all['4_change_abs']=df_all['pct_chg'].abs()+df_all['y_1_chg'].abs()+df_all['y_2_chg'].abs()+df_all['y_3_chg'].abs()


        #计算三种比例rank
        dolist=['y_1_chg','y_2_chg','y_3_chg','4_change_abs']

        for curc in dolist:
            df_all[curc]=df_all.groupby('trade_date')[curc].rank(pct=True)
            df_all[curc]=df_all[curc]*10//2

        df_all.drop(['y_1_chg','y_2_chg','y_3_chg'],axis=1,inplace=True)

        #6日
        xxx=df_all.groupby('ts_code')['chg_rank'].rolling(6).sum().reset_index()
        xxx.set_index(['level_1'], drop=True, append=False, inplace=True, verify_integrity=False)
        xxx.drop(['ts_code'],axis=1,inplace=True)

        df_all=df_all.join(xxx, lsuffix='', rsuffix='_6')

        df_all['chg_rank_6']=df_all.groupby('trade_date')['chg_rank_6'].rank(pct=True)
        df_all['chg_rank_6']=df_all['chg_rank_6']*10//2

        #10日
        xxx=df_all.groupby('ts_code')['chg_rank'].rolling(10).sum().reset_index()
        xxx.set_index(['level_1'], drop=True, append=False, inplace=True, verify_integrity=False)
        xxx.drop(['ts_code'],axis=1,inplace=True)

        df_all=df_all.join(xxx, lsuffix='', rsuffix='_10')

        df_all['chg_rank_10']=df_all.groupby('trade_date')['chg_rank_10'].rank(pct=True)
        df_all['chg_rank_10']=df_all['chg_rank_10']*10//2

        #3日
        xxx=df_all.groupby('ts_code')['chg_rank'].rolling(3).sum().reset_index()
        xxx.set_index(['level_1'], drop=True, append=False, inplace=True, verify_integrity=False)
        xxx.drop(['ts_code'],axis=1,inplace=True)

        df_all=df_all.join(xxx, lsuffix='', rsuffix='_3')

        df_all['chg_rank_3']=df_all.groupby('trade_date')['chg_rank_3'].rank(pct=True)
        df_all['chg_rank_3']=df_all['chg_rank_3']*10//2

        #20日
        xxx=df_all.groupby('ts_code')['chg_rank'].rolling(20).sum().reset_index()
        xxx.set_index(['level_1'], drop=True, append=False, inplace=True, verify_integrity=False)
        xxx.drop(['ts_code'],axis=1,inplace=True)

        df_all=df_all.join(xxx, lsuffix='', rsuffix='_20')

        df_all['chg_rank_20']=df_all.groupby('trade_date')['chg_rank_3'].rank(pct=True)
        df_all['chg_rank_20']=df_all['chg_rank_20']*10//1

        #print(df_all)

        ##当日量排名
        #df_all['amount_rank']=df_all.groupby('trade_date')['amount'].rank(pct=True)
        #df_all['amount_rank']=df_all['amount_rank']*10//2

        #10日均量
        xxx=df_all.groupby('ts_code')['amount'].rolling(10).mean().reset_index()
        xxx.set_index(['level_1'], drop=True, append=False, inplace=True, verify_integrity=False)
        xxx.drop(['ts_code'],axis=1,inplace=True)
        df_all=df_all.join(xxx, lsuffix='', rsuffix='_10')

        #当日量占比
        df_all['pst_amount']=df_all['amount']/df_all['amount_10']
        #10日量排名
        df_all['rank_amount_10']=df_all.groupby('trade_date')['amount_10'].rank(pct=True)
        #df_all['amount_10_rank']=df_all['rank_amount_10']*10//2

        df_all.drop(['amount_10'],axis=1,inplace=True)
        #当日量排名
        df_all['pst_amount_rank_10']=df_all.groupby('trade_date')['pst_amount'].rank(pct=True)
        df_all['pst_amount_rank_10']=df_all['pst_amount_rank_10']*10//2

        ##5日均量
        #xxx=df_all.groupby('ts_code')['amount'].rolling(5).mean().reset_index()
        #xxx.set_index(['level_1'], drop=True, append=False, inplace=True, verify_integrity=False)
        #xxx.drop(['ts_code'],axis=1,inplace=True)
        #df_all=df_all.join(xxx, lsuffix='', rsuffix='_5')

        ##当日量占比
        #df_all['pst_amount']=df_all['amount']/df_all['amount_5']
        #df_all.drop(['amount_5'],axis=1,inplace=True)
        ##当日量排名
        #df_all['pst_amount_rank_5']=df_all.groupby('trade_date')['pst_amount'].rank(pct=True)
        #df_all['pst_amount_rank_5']=df_all['pst_amount_rank_5']*10//1

        #计算三种比例rank
        dolist=['open','high','low']

        for curc in dolist:
            buffer=((df_all[curc]-df_all['pre_close'])*100)/df_all['close']
            df_all[curc]=buffer
            df_all[curc]=df_all.groupby('trade_date')[curc].rank(pct=True)
            df_all[curc]=df_all[curc]*10//2

        #加入昨日rank
        df_all['yesterday_open']=df_all.groupby('ts_code')['open'].shift(1)
        df_all['yesterday_high']=df_all.groupby('ts_code')['high'].shift(1)
        df_all['yesterday_low']=df_all.groupby('ts_code')['low'].shift(1)
        df_all['yesterday_pst_amount_rank']=df_all.groupby('ts_code')['pst_amount_rank_10'].shift(2)
        ##加入前日open
        df_all['yesterday2_open']=df_all.groupby('ts_code')['open'].shift(2)
        df_all['yesterday2_high']=df_all.groupby('ts_code')['high'].shift(2)
        df_all['yesterday2_low']=df_all.groupby('ts_code')['low'].shift(2)
        df_all['yesterday2_pst_amount_rank']=df_all.groupby('ts_code')['pst_amount_rank_10'].shift(2)
        ##加入前日open
        df_all['yesterday3_open']=df_all.groupby('ts_code')['open'].shift(3)
        df_all['yesterday3_high']=df_all.groupby('ts_code')['high'].shift(3)
        df_all['yesterday3_low']=df_all.groupby('ts_code')['low'].shift(3)
        df_all['yesterday3_pst_amount_rank']=df_all.groupby('ts_code')['pst_amount_rank_10'].shift(3)

        df_all.drop(['close','pre_close','pct_chg','pst_amount','adj_factor','real_price','amount'],axis=1,inplace=True)
        #df_all.drop(['close','pre_close','pct_chg','adj_factor','real_price'],axis=1,inplace=True)

        #暂时不用的列
        df_all=df_all[df_all['high_stop']==0]

        #'tomorrow_chg'
        df_all.drop(['high_stop'],axis=1,inplace=True) 

        df_all.dropna(axis=0,how='any',inplace=True)

        print(df_all)
        df_all=df_all.reset_index(drop=True)

        return df_all

    def real_FE(self,gap_day):

        df_data=pd.read_csv('real_now.csv',index_col=0,header=0)
        df_adj_all=pd.read_csv('real_adj_now.csv',index_col=0,header=0)

        df_all=pd.merge(df_data, df_adj_all, how='left', on=['ts_code','trade_date'])

        #加入间隔
        df_all['gap_day']=gap_day

        #df_all=pd.read_csv(bufferstring,index_col=0,header=0,nrows=100000)
    
        #df_all.drop(['change','vol'],axis=1,inplace=True)
 

        #===================================================================================================================================#

        #复权后价格
        df_all['adj_factor']=df_all['adj_factor'].fillna(0)
        df_all['real_price']=df_all['close']*df_all['adj_factor']
        
        df_all['real_price']=df_all.groupby('ts_code')['real_price'].shift(1)
        df_all['real_price']=df_all['real_price']*(1+df_all['pct_chg']/100)


        #30日最低比值
        xxx=df_all.groupby('ts_code')['real_price'].rolling(30).min().reset_index()
        xxx.set_index(['level_1'], drop=True, append=False, inplace=True, verify_integrity=False)
        xxx.drop(['ts_code'],axis=1,inplace=True)
        
        df_all=df_all.join(xxx, lsuffix='', rsuffix='_30min')
        #bbb=df_all.groupby('ts_code')['real_price'].agg({'all_min':np.min, 'all_max': np.max}).reset_index()
        #ccc=pd.merge(df_all, bbb, how='inner', on=['ts_code'])
        df_all['30_pct']=(df_all['real_price']-df_all['real_price_30min'])/df_all['real_price_30min']
        df_all['30_pct_rank']=df_all.groupby('trade_date')['30_pct'].rank(pct=True)
        df_all['30_pct_rank']=df_all['30_pct_rank']*10//1

        #30日最高比值
        xxx=df_all.groupby('ts_code')['real_price'].rolling(30).max().reset_index()
        xxx.set_index(['level_1'], drop=True, append=False, inplace=True, verify_integrity=False)
        xxx.drop(['ts_code'],axis=1,inplace=True)
        
        df_all=df_all.join(xxx, lsuffix='', rsuffix='_30max')
        #bbb=df_all.groupby('ts_code')['real_price'].agg({'all_min':np.min, 'all_max': np.max}).reset_index()
        #ccc=pd.merge(df_all, bbb, how='inner', on=['ts_code'])
        df_all['30_pct_max']=(df_all['real_price']-df_all['real_price_30max'])/df_all['real_price_30max']
        df_all['30_pct_max_rank']=df_all.groupby('trade_date')['30_pct_max'].rank(pct=True)
        df_all['30_pct_max_rank']=df_all['30_pct_max_rank']*10//1

        df_all.drop(['30_pct','real_price_30min','30_pct_max','real_price_30max'],axis=1,inplace=True)


        #===================================================================================================================================#


        #是否停
        df_all['high_stop']=0
        df_all.loc[df_all['pct_chg']>9,'high_stop']=1
        df_all.loc[(df_all['pct_chg']<5.5) & (4.5<df_all['pct_chg']),'high_stop']=1


        #真实价格范围
        df_all['price_real_rank']=df_all.groupby('trade_date')['pre_close'].rank(pct=True)
        df_all['price_real_rank']=df_all['price_real_rank']*10//1
        #1日
        df_all['chg_rank']=df_all.groupby('trade_date')['pct_chg'].rank(pct=True)
        df_all['chg_rank']=df_all['chg_rank']*10//1


        #6日
        xxx=df_all.groupby('ts_code')['chg_rank'].rolling(6).sum().reset_index()
        xxx.set_index(['level_1'], drop=True, append=False, inplace=True, verify_integrity=False)
        xxx.drop(['ts_code'],axis=1,inplace=True)

        df_all=df_all.join(xxx, lsuffix='', rsuffix='_6')

        df_all['chg_rank_6']=df_all.groupby('trade_date')['chg_rank_6'].rank(pct=True)
        df_all['chg_rank_6']=df_all['chg_rank_6']*10//1

        #10日
        xxx=df_all.groupby('ts_code')['chg_rank'].rolling(10).sum().reset_index()
        xxx.set_index(['level_1'], drop=True, append=False, inplace=True, verify_integrity=False)
        xxx.drop(['ts_code'],axis=1,inplace=True)

        df_all=df_all.join(xxx, lsuffix='', rsuffix='_10')

        df_all['chg_rank_10']=df_all.groupby('trade_date')['chg_rank_10'].rank(pct=True)
        df_all['chg_rank_10']=df_all['chg_rank_10']*10//1

        #3日
        xxx=df_all.groupby('ts_code')['chg_rank'].rolling(3).sum().reset_index()
        xxx.set_index(['level_1'], drop=True, append=False, inplace=True, verify_integrity=False)
        xxx.drop(['ts_code'],axis=1,inplace=True)

        df_all=df_all.join(xxx, lsuffix='', rsuffix='_3')

        df_all['chg_rank_3']=df_all.groupby('trade_date')['chg_rank_3'].rank(pct=True)
        df_all['chg_rank_3']=df_all['chg_rank_3']*10//1

        #print(df_all)

        #10日均量
        xxx=df_all.groupby('ts_code')['amount'].rolling(10).mean().reset_index()
        xxx.set_index(['level_1'], drop=True, append=False, inplace=True, verify_integrity=False)
        xxx.drop(['ts_code'],axis=1,inplace=True)
        df_all=df_all.join(xxx, lsuffix='_1', rsuffix='_10')

        #当日量占比
        df_all['pst_amount']=df_all['amount_1']/df_all['amount_10']
        df_all.drop(['amount_1','amount_10'],axis=1,inplace=True)
        #当日量排名
        df_all['pst_amount_rank']=df_all.groupby('trade_date')['pst_amount'].rank(pct=True)
        df_all['pst_amount_rank']=df_all['pst_amount_rank']*10//1

        #计算三种比例rank
        dolist=['open','high','low']

        for curc in dolist:
            buffer=((df_all[curc]-df_all['pre_close'])*100)/df_all['pre_close']
            df_all[curc]=buffer
            df_all[curc]=df_all.groupby('trade_date')[curc].rank(pct=True)
            df_all[curc]=df_all[curc]*10//1

        #加入昨日rank
        df_all['yesterday_open']=df_all.groupby('ts_code')['open'].shift(1)
        df_all['yesterday_high']=df_all.groupby('ts_code')['high'].shift(1)
        df_all['yesterday_low']=df_all.groupby('ts_code')['low'].shift(1)
        df_all['yesterday_pst_amount_rank']=df_all.groupby('ts_code')['pst_amount_rank'].shift(1)
        #加入前日open
        df_all['yesterday2_open']=df_all.groupby('ts_code')['open'].shift(2)

        df_all.drop(['close','pre_close','pct_chg','pst_amount','adj_factor','real_price'],axis=1,inplace=True)
        #暂时不用的列
        df_all=df_all[df_all['high_stop']==0]
        #'tomorrow_chg'
        df_all.drop(['high_stop'],axis=1,inplace=True)



        df_all.dropna(axis=0,how='any',inplace=True)


        month_sec=df_all['trade_date'].max()
        df_all=df_all[df_all['trade_date']!=month_sec]
        print(df_all)
        df_all=df_all.reset_index(drop=True)

        df_all.to_csv('today_train.csv')
        dwdw=1