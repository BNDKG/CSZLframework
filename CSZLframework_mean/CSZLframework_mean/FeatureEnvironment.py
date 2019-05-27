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