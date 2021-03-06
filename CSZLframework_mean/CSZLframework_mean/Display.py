#coding=utf-8
import pandas as pd
import numpy as np
import matplotlib
import matplotlib.pyplot as plt
import tushare as ts
import os

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

        days,show3=self.show_all_rate_ens_plus(path)

        #plt.plot(days,show1,c='blue',label="000001")
        #plt.plot(days,show2,c='red',label="399006")
        plt.plot(days,show3,c='green',label="my model head10mean")

        plt.legend()

        plt.show()

        input()


    def parafirst(self,path):
        new_train_times=4
        showsource_list=[]
        for counter in range(new_train_times):
            path_new=os.path.splitext(path)[0]+str(counter)+".csv"

            ss=pd.read_csv(path_new,index_col=0,header=0)           
            
            showsource_list.append(ss)

        return showsource_list

    def paramain(self,showsource_get,Y):
            
        showsource_list=[]
        new_train_times=4
        for counter in range(new_train_times):
            ss=showsource_get[counter]
            ss['mix']=ss['0']*Y[0]+ss['1']*Y[1]+ss['2']*Y[2]+ss['3']*Y[3]+ss['4']*Y[4]+ss['5']*Y[5]+ss['6']*Y[6]+ss['7']*Y[7]+ss['8']*Y[8]+ss['9']*Y[9]
            #ss['mix']=ss['0']*Y[0]+ss['1']*Y[1]+ss['2']*Y[2]+ss['3']*Y[3]+ss['4']*Y[4]+ss['5']*Y[5]+ss['6']*Y[6]+ss['7']*Y[7]+ss['8']*Y[8]+ss['9']*Y[9]+ss['10']*Y[10]+ss['11']*Y[11]+ss['12']*Y[12]+ss['13']*Y[13]+ss['14']*Y[14]
            #ss['mix']=ss['0']*Y[0]+ss['1']*Y[1]+ss['2']*Y[2]+ss['3']*Y[3]+ss['4']*Y[4]
            showsource_list.append(ss)
            

        #showsource_list[0]['mix']=showsource_list[0].groupby('trade_date')['mix'].rank(ascending=False)
        showsource=showsource_list[0]
        for counter in range(new_train_times):
            if counter==0:
                continue
            #showsource['mix']=showsource['mix']+showsource_list[counter].groupby('trade_date')['mix'].rank(ascending=False)
            showsource['mix']=showsource['mix']+showsource_list[counter]['mix']
            sfasfd=12
            
        #print(showsource['mix'])
        databuffer=showsource['trade_date'].unique()


        #showsource['mix_rank']=showsource.groupby('trade_date')['mix'].rank(ascending=False)
        #showsource['next_chg']=showsource.groupby('ts_code')['tomorrow_chg'].shift(-1)
        changer=[]
        for curdata in databuffer:

            cur_show=showsource[showsource["trade_date"]==curdata]
            b=cur_show.sort_values(by="mix" , ascending=False)
            #b=cur_show.sort_values(by="9" , ascending=True)
            #d=b.head(10)
            #e=d.sort_values(by="mix" , ascending=True)
            

            #buffer=b.head(10)
            #buffer2=buffer.sort_values(by="9" , ascending=False)
            #average=buffer2.head(1)['tomorrow_chg'].mean()

            #b=cur_show[cur_show['mix']>0.40]
            average=b.head(15)['tomorrow_chg'].mean()
            #average=b.tail(10)['tomorrow_chg'].mean()
            changer.append(average)

            adwda=1


        days2,show=self.standard_show_para(changer,day_interval=5)
        #days2,show=self.standard_show_Kelly_Criterion(changer,day_interval=1)
        
        #showsource=showsource[showsource['mix_rank']<10]
        #showsource.to_csv('seefef.csv')
        return show[-1]

    def real_plot_show(self):

        file_dir = "./temp3"
        a = os.walk(file_dir)
        path_list=[]
        for root, dirs, files in os.walk(file_dir):  
            path_list=files
            break
        changer=[]
        for curdata in path_list:

            cur_path="./temp3/"+curdata
            cur_df=pd.read_csv(cur_path,index_col=0,header=0)

            b=cur_df.sort_values(by="mix" , ascending=False)

            #buffer=b.head(10)
            #buffer2=buffer.sort_values(by="9" , ascending=False)
            #average=buffer2.head(1)['pct_chg'].mean()


            #average=b.tail(3)['pct_chg'].mean()
            average=b.head(10)['pct_chg'].mean()

            changer.append(average)
            adwda=1


        days2,show=self.standard_show(changer,day_interval=1)

        #plt.plot(days,show1,c='blue',label="000001")
        #plt.plot(days,show2,c='red',label="399006")
        plt.plot(days2,show,c='green',label="my model real")

        plt.legend()

        plt.show()

    def real_plot_show_plus(self):

        new_train_times=4

        #Y=[-12,-8,-3,-2,-1,1,2,3,12,18]

        Y=[-12,-6,-3,-2,-1,1,2,3,6,12]

        all_csv_path=pd.read_csv('./Database/Dailydata.csv',index_col=0,header=0)
        all_csv_path=all_csv_path.loc[:,['ts_code','trade_date','pct_chg']]
        all_csv_path=all_csv_path[-1000000:]
        all_csv_path['pct_chg']=all_csv_path['pct_chg'].astype('float64')

        #all_csv_path['pct_chg']=all_csv_path.groupby('ts_code')['pct_chg'].shift(-1)

        #明日幅度
        tm1=all_csv_path.groupby('ts_code')['pct_chg'].shift(-1)
        tm2=all_csv_path.groupby('ts_code')['pct_chg'].shift(-2)
        #df_all['tomorrow_chg']=((100+tm1)*(100+tm2)-10000)/100
        tm3=all_csv_path.groupby('ts_code')['pct_chg'].shift(-3)
        tm4=all_csv_path.groupby('ts_code')['pct_chg'].shift(-4)
        tm5=all_csv_path.groupby('ts_code')['pct_chg'].shift(-5)
        #tm6=all_csv_path.groupby('ts_code')['pct_chg'].shift(-6)
        #tm7=all_csv_path.groupby('ts_code')['pct_chg'].shift(-7)
        ##df_all['tomorrow_chg']=((100+tm1)*(100+tm2)-10000)/100
        #tm8=all_csv_path.groupby('ts_code')['pct_chg'].shift(-8)
        #tm9=all_csv_path.groupby('ts_code')['pct_chg'].shift(-9)
        #tm10=all_csv_path.groupby('ts_code')['pct_chg'].shift(-10)

        #all_csv_path['pct_chg']=(((100+tm1)/100)*((100+tm2)/100)*((100+tm3)/100)*((100+tm4)/100)*((100+tm5)/100)*
        #                         ((100+tm6)/100)*((100+tm7)/100)*((100+tm8)/100)*((100+tm9)/100)*((100+tm10)/100)-1)*100
        all_csv_path['pct_chg']=(((100+tm1)/100)*((100+tm2)/100)*((100+tm3)/100)*((100+tm4)/100)*((100+tm5)/100)-1)*100
        #all_csv_path['pct_chg']=(((100+tm1)/100)-1)*100

        file_dir = "./temp3"
        a = os.walk(file_dir)
        path_list=[]
        for root, dirs, files in os.walk(file_dir):  
            path_list=files
            break

        firstdata="./temp3/"+path_list[0]

        all_real_data=pd.read_csv(firstdata,index_col=0,header=0)
        print(all_real_data)

        for curdata in path_list:
            if(curdata==path_list[0]):
                continue

            cur_path="./temp3/"+curdata
            cur_df=pd.read_csv(cur_path,index_col=0,header=0)

            all_real_data=pd.concat([all_real_data,cur_df],axis=0)

            adwda=1

        all_real_data=all_real_data.rename(columns = {"trade_date_x": "trade_date"})
        #all_real_data['trade_date']=all_real_data['trade_date'].astype('object')

        #all_csv_path['ts_code']=all_csv_path['ts_code'].astype('int64')

        all_csv_path['ts_code']=all_csv_path['ts_code'].map(lambda x : x[:-3])
        all_csv_path['ts_code']=all_csv_path['ts_code'].astype('int64')

        showsource=pd.merge(all_real_data, all_csv_path, how='left', on=['ts_code','trade_date'])

        databuffer=showsource['trade_date'].unique()

        #showsource['mix_rank']=showsource.groupby('trade_date')['mix'].rank(ascending=False,pct=False,method='first')
        #print(showsource)
        #showsource['next_chg']=showsource.groupby('ts_code')['tomorrow_chg'].shift(-1)
        changer=[]
        for curdata in databuffer:

            cur_show=showsource[showsource["trade_date"]==curdata]
            b=cur_show.sort_values(by="mix" , ascending=False)
            #b=cur_show.sort_values(by="9" , ascending=True)
            #d=b.head(10)
            #e=d.sort_values(by="mix" , ascending=True)
            

            #buffer=b.tail(20)
            #buffer2=buffer.sort_values(by="0" , ascending=False)
            #average=buffer2.tail(1)['tomorrow_chg'].mean()

            #b=cur_show[cur_show['mix']>0.40]
            average=b.head(3)['pct_chg_y'].mean()
            #average=b.head(3)['tomorrow_chg'].mean()
            #average=b.tail(15)['tomorrow_chg'].mean()
            #if(average>10):
            #    sdsdf=1
                
            changer.append(average)

            adwda=1
        print(changer)

        days2,show=self.standard_show(changer,day_interval=5)

        #plt.plot(days,show1,c='blue',label="000001")
        #plt.plot(days,show2,c='red',label="399006")
        plt.plot(days2,show,c='green',label="my model real")

        plt.legend()

        plt.show()

        zfseseg=1

    def real_plot_create(self):

        file_dir = "./temp2"
        a = os.walk(file_dir)
        path_list=[]
        for root, dirs, files in os.walk(file_dir):  
            path_list=dirs
            break

        cur=path_list[0][6:]
        for curpath in path_list:
            next=curpath[6:]
            if(next!=cur):
                self._remix_csv(cur,next)

            cur=next
            sdsd=1
        asdad=1

    def _remix_csv(self,predit_date,next_date):

        result_path='./temp3/'+next_date+'.csv'
        isExists=os.path.exists(result_path)
        # 判断结果
        if isExists:
            print(result_path+' 文件已存在')
            return

        remixpath='./temp2/result'+predit_date

        show=pd.read_csv(remixpath+'/out1.csv',index_col=0,header=0)
        show2=pd.read_csv(remixpath+'/out2.csv',index_col=0,header=0)
        show3=pd.read_csv(remixpath+'/out3.csv',index_col=0,header=0)
        show4=pd.read_csv(remixpath+'/out4.csv',index_col=0,header=0)

        #show['mix']=show['mix'].rank(ascending=True)
        #show2['mix']=show2['mix'].rank(ascending=True)
        #show3['mix']=show3['mix'].rank(ascending=True)
        #show4['mix']=show4['mix'].rank(ascending=True)

        show['9']=show['9']+show2['9']+show3['9']+show4['9']
        show['mix']=show['mix']+show2['mix']+show3['mix']+show4['mix']
        
        #读取token
        f = open('token.txt')
        token = f.read()     #将txt文件的所有内容读入到字符串str中
        f.close()
        pro = ts.pro_api(token)

        df = pro.daily(trade_date=next_date)
        print(df)
        df['ts_code']=df['ts_code'].map(lambda x : x[:-3])

        df['ts_code'] = df['ts_code'].astype(int64)
        print(df)
        result = pd.merge(show, df, how='left', on=['ts_code'])


        result.to_csv(result_path)

        fsfef=1

    def show_all_rate(self,path):
        showsource=pd.read_csv(path,index_col=0,header=0)
        databuffer=showsource['trade_date'].unique()

        showsource['mix']=showsource['0']*(-8)+showsource['1']*(-8)+showsource['2']*(-3)+showsource['3']*(-2)+showsource['4']*(-1)+showsource['5']*1+showsource['6']*2+showsource['7']*3+showsource['8']*7+showsource['9']*12
        #multlist=[-12,-5,-3,-2,-1.5,-1,-0.75,-0.5,-0.25,0,0,0.25,0.5,0.75,1,1.5,2,3,5,12]
        showsource['mix_rank']=showsource.groupby('trade_date')['mix'].rank(ascending=False)
        showsource['next_chg']=showsource.groupby('ts_code')['tomorrow_chg'].shift(-1)
        changer=[]
        for curdata in databuffer:

            cur_show=showsource[showsource["trade_date"]==curdata]
            b=cur_show.sort_values(by="mix" , ascending=False)
            #b=cur_show.sort_values(by="9" , ascending=True)
            #d=b.head(10)
            #e=d.sort_values(by="mix" , ascending=True)
            

            #b=cur_show[cur_show['mix']>0.40]
            average=b.head(50)['tomorrow_chg'].mean()
            changer.append(average)

            adwda=1


        days2,show=self.standard_show(changer,day_interval=1)
        showsource=showsource[showsource['mix_rank']<10]
        showsource.to_csv('seefef.csv')
        return days2,show

    def show_all_rate_ens(self,path):

        new_train_times=4
        #Y=[-9,-2,-4,-2,-1,1,2,-10,15,8]
        #Y=[-17,25,-23,0,0,0,0,4,12,25]
        Y=[-8,-8,-3,-2,-1,1,2,3,4,12]
        #Y=[-8,-8,-3,-2,-1,0,0,0,0,0,1,2,3,7,12]
        #Y=[-35,-10,0,0,0,0,0,0,20,44]
        #单日最高
        #Y=[-17,8,-9,0,0,0,0,-1,20,25]
        #Y=[-9,-4,0,0,0,0,0,0,14,25]
        

        #凯利公式
        #Y=[0,-31,45,0,0,0,0,22,13,25]
        #凯莉反向最佳
        #Y=[-1,42,-48,0,0,0,0,50,38,25]

        #Y=[-4,-30,15,0,0,0,0,25,12,25]
        #Y=[-19,-5,-24,0,0,0,0,2,9,25]

        showsource_list=[]
        for counter in range(new_train_times):
            path_new=os.path.splitext(path)[0]+str(counter)+".csv"

            ss=pd.read_csv(path_new,index_col=0,header=0)
            #ss['mix']=ss['0']*Y[0]+ss['1']*Y[1]+ss['2']*Y[2]+ss['3']*Y[3]+ss['4']*Y[4]+ss['5']*Y[5]+ss['6']*Y[6]+ss['7']*Y[7]+ss['8']*Y[8]+ss['9']*Y[9]+ss['10']*Y[10]+ss['11']*Y[11]+ss['12']*Y[12]+ss['13']*Y[13]+ss['14']*Y[14]
            ss['mix']=ss['0']*Y[0]+ss['1']*Y[1]+ss['2']*Y[2]+ss['3']*Y[3]+ss['4']*Y[4]+ss['5']*Y[5]+ss['6']*Y[6]+ss['7']*Y[7]+ss['8']*Y[8]+ss['9']*Y[9]
            #ss['mix']=ss['0']*Y[0]+ss['1']*Y[1]+ss['2']*Y[2]+ss['3']*Y[3]+ss['4']*Y[4]
            showsource_list.append(ss)
        
        #showsource_list[0]['mix']=showsource_list[0].groupby('trade_date')['mix'].rank(ascending=False)
        showsource=showsource_list[0]
        for counter in range(new_train_times):
            if counter==0:
                continue
            #showsource['mix']=showsource['mix']+showsource_list[counter].groupby('trade_date')['mix'].rank(ascending=False)
            showsource['mix']=showsource['mix']+showsource_list[counter]['mix']
            sfasfd=12
            
        print(showsource['mix'])
        databuffer=showsource['trade_date'].unique()

        showsource['mix_rank']=showsource.groupby('trade_date')['mix'].rank(ascending=False,pct=False,method='first')
        print(showsource)
        showsource['next_chg']=showsource.groupby('ts_code')['tomorrow_chg'].shift(-1)
        changer=[]
        for curdata in databuffer:

            cur_show=showsource[showsource["trade_date"]==curdata]
            b=cur_show.sort_values(by="mix" , ascending=False)
            #b=cur_show.sort_values(by="9" , ascending=True)
            #d=b.head(10)
            #e=d.sort_values(by="mix" , ascending=True)
            

            #buffer=b.tail(20)
            #buffer2=buffer.sort_values(by="0" , ascending=False)
            #average=buffer2.tail(1)['tomorrow_chg'].mean()

            #b=cur_show[cur_show['mix']>0.40]
            average=b.head(1)['tomorrow_chg'].mean()
            #average=b.tail(1)['tomorrow_chg'].mean()
            if(average>10):
                sdsdf=1
                
            changer.append(average)

            adwda=1
        print(changer)

        days2,show=self.standard_show(changer,day_interval=5)
        #days2,show=self.standard_show_Kelly_Criterion(changer,day_interval=5)
        
        showsource=showsource[showsource['mix_rank']<10]
        showsource.to_csv('seefef.csv')
        return days2,show

    def show_all_rate_ens_plus(self,path):

        new_train_times=4
        #Y=[-9,-2,-4,-2,-1,1,2,-10,15,8]
        #Y=[-17,25,-23,0,0,0,0,4,12,25]
        #Y=[-24,-6,0,0,0,0,0,0,41,41]
        Y=[-12,-8,-3,-2,-1,1,2,3,12,18]
        #Y=[-12,-6,-3,-2,-1,1,2,3,6,12]
        #Y=[-8,-8,-3,-2,-1,0,0,0,0,0,1,2,3,7,12]
        #Y=[-35,-10,0,0,0,0,0,0,20,44]

        #单日最高
        #Y=[-17,8,-9,0,0,0,0,-1,20,25]
        #Y=[-9,-4,0,0,0,0,0,0,14,25]
        

        #凯利公式
        #Y=[0,-31,45,0,0,0,0,22,13,25]
        #凯莉反向最佳
        #Y=[-1,42,-48,0,0,0,0,50,38,25]

        #Y=[-4,-30,15,0,0,0,0,25,12,25]
        #Y=[-19,-5,-24,0,0,0,0,2,9,25]

        all_csv_path=pd.read_csv('./Database/Dailydata.csv',index_col=0,header=0)
        all_csv_path=all_csv_path.loc[:,['ts_code','trade_date','pct_chg']]
        all_csv_path['pct_chg']=all_csv_path['pct_chg'].astype('float64')

        #all_csv_path['pct_chg']=all_csv_path.groupby('ts_code')['pct_chg'].shift(-1)

        #明日幅度
        tm1=all_csv_path.groupby('ts_code')['pct_chg'].shift(-1)
        tm2=all_csv_path.groupby('ts_code')['pct_chg'].shift(-2)
        #df_all['tomorrow_chg']=((100+tm1)*(100+tm2)-10000)/100
        tm3=all_csv_path.groupby('ts_code')['pct_chg'].shift(-3)
        tm4=all_csv_path.groupby('ts_code')['pct_chg'].shift(-4)
        tm5=all_csv_path.groupby('ts_code')['pct_chg'].shift(-5)
        #tm6=all_csv_path.groupby('ts_code')['pct_chg'].shift(-6)
        #tm7=all_csv_path.groupby('ts_code')['pct_chg'].shift(-7)
        ##df_all['tomorrow_chg']=((100+tm1)*(100+tm2)-10000)/100
        #tm8=all_csv_path.groupby('ts_code')['pct_chg'].shift(-8)
        #tm9=all_csv_path.groupby('ts_code')['pct_chg'].shift(-9)
        #tm10=all_csv_path.groupby('ts_code')['pct_chg'].shift(-10)

        #all_csv_path['pct_chg']=(((100+tm1)/100)*((100+tm2)/100)*((100+tm3)/100)*((100+tm4)/100)*((100+tm5)/100)*
        #                         ((100+tm6)/100)*((100+tm7)/100)*((100+tm8)/100)*((100+tm9)/100)*((100+tm10)/100)-1)*100
        all_csv_path['pct_chg']=(((100+tm1)/100)*((100+tm2)/100)*((100+tm3)/100)*((100+tm4)/100)*((100+tm5)/100)-1)*100
        #all_csv_path['pct_chg']=(((100+tm1)/100)-1)*100

        showsource_list=[]
        for counter in range(new_train_times):
            path_new=os.path.splitext(path)[0]+str(counter)+".csv"

            ss=pd.read_csv(path_new,index_col=0,header=0)
            #ss['mix']=ss['0']*Y[0]+ss['1']*Y[1]+ss['2']*Y[2]+ss['3']*Y[3]+ss['4']*Y[4]+ss['5']*Y[5]+ss['6']*Y[6]+ss['7']*Y[7]+ss['8']*Y[8]+ss['9']*Y[9]+ss['10']*Y[10]+ss['11']*Y[11]+ss['12']*Y[12]+ss['13']*Y[13]+ss['14']*Y[14]
            ss['mix']=ss['0']*Y[0]+ss['1']*Y[1]+ss['2']*Y[2]+ss['3']*Y[3]+ss['4']*Y[4]+ss['5']*Y[5]+ss['6']*Y[6]+ss['7']*Y[7]+ss['8']*Y[8]+ss['9']*Y[9]
            #ss['mix']=ss['0']*Y[0]+ss['1']*Y[1]+ss['2']*Y[2]+ss['3']*Y[3]+ss['4']*Y[4]
            showsource_list.append(ss)
        
        #showsource_list[0]['mix']=showsource_list[0].groupby('trade_date')['mix'].rank(ascending=False)
        showsource=showsource_list[0]
        for counter in range(new_train_times):
            if counter==0:
                continue
            #showsource['mix']=showsource['mix']+showsource_list[counter].groupby('trade_date')['mix'].rank(ascending=False)
            showsource['mix']=showsource['mix']+showsource_list[counter]['mix']
            sfasfd=12
            
        showsource=pd.merge(showsource, all_csv_path, how='left', on=['ts_code','trade_date'])
        print(showsource['mix'])
        databuffer=showsource['trade_date'].unique()

        showsource['mix_rank']=showsource.groupby('trade_date')['mix'].rank(ascending=False,pct=False,method='first')
        print(showsource)
        showsource['next_chg']=showsource.groupby('ts_code')['tomorrow_chg'].shift(-1)
        changer=[]
        for curdata in databuffer:

            cur_show=showsource[showsource["trade_date"]==curdata]
            b=cur_show.sort_values(by="mix" , ascending=False)
            #b=cur_show.sort_values(by="9" , ascending=True)
            #d=b.head(10)
            #e=d.sort_values(by="mix" , ascending=True)
            

            #buffer=b.tail(20)
            #buffer2=buffer.sort_values(by="0" , ascending=False)
            #average=buffer2.tail(1)['tomorrow_chg'].mean()

            #b=cur_show[cur_show['mix']>0.40]
            average=b.head()['pct_chg'].mean()
            #average=b.head(3)['tomorrow_chg'].mean()
            #average=b.tail(15)['tomorrow_chg'].mean()
            #if(average>10):
            #    sdsdf=1
                
            changer.append(average)

            adwda=1
        print(changer)

        days2,show=self.standard_show(changer,day_interval=5)
        #days2,show=self.standard_show_Kelly_Criterion(changer,first_base_income=100000,day_interval=15)
        
        showsource=showsource[showsource['mix_rank']<10]
        showsource.to_csv('seefef.csv')
        return days2,show

    def show_all_rate_ens_old(self,path):
        showsource=pd.read_csv(path,index_col=0,header=0)
        databuffer=showsource['trade_date'].unique()

        #showsource2=pd.read_csv('./temp/1111.csv',index_col=0,header=0)
        #showsource3=pd.read_csv('./temp/2222.csv',index_col=0,header=0)
        #showsource4=pd.read_csv('./temp/0000.csv',index_col=0,header=0)

        ##print(showsource['mix'])
        #showsource['mix']=showsource['mix']*1+showsource2['mix']*1+showsource3['mix']*1+showsource4['mix']*1
        print(showsource['mix'])
        multlist=[-12,-5,-3,-2,-1.5,-1,-0.75,-0.5,-0.25,0,0,0.25,0.5,0.75,1,1.5,2,3,5,13]
        showsource['mix_rank']=showsource.groupby('trade_date')['mix'].rank(ascending=False)
        showsource['next_chg']=showsource.groupby('ts_code')['tomorrow_chg'].shift(-1)
        changer=[]
        for curdata in databuffer:

            cur_show=showsource[showsource["trade_date"]==curdata]
            b=cur_show.sort_values(by="mix" , ascending=False)
            #b=cur_show.sort_values(by="9" , ascending=True)
            #d=b.head(10)
            #e=d.sort_values(by="mix" , ascending=True)
            

            #buffer=b.head(10)
            #buffer2=buffer.sort_values(by="9" , ascending=False)
            #average=buffer2.head(1)['tomorrow_chg'].mean()

            #b=cur_show[cur_show['mix']>0.40]
            average=b.head(1)['tomorrow_chg'].mean()
            changer.append(average)

            adwda=1


        days2,show=self.standard_show(changer,day_interval=1)
        showsource=showsource[showsource['mix_rank']<10]
        showsource.to_csv('seefef.csv')
        return days2,show

    def standard_show(self,changer,first_base_income=100000,day_interval=2,label="自己"):
    
        start_from=first_base_income
        show=[]
        for curchange in changer:
            start_from=start_from+(first_base_income/100/day_interval)*curchange-0.0020*first_base_income/day_interval
            show.append(start_from)

        #print(show)
        len_show=len(show)
        days=np.arange(1,len_show+1)

        fig=plt.figure(figsize=(6,3))


        #plt.show()

        return days,show

    def standard_show_para(self,changer,first_base_income=100000,day_interval=2):
    
        start_from=first_base_income
        show=[]
        for curchange in changer:
            start_from=start_from+(first_base_income/100/day_interval)*curchange-0.0020*first_base_income/day_interval
            show.append(start_from)

        #print(show)
        len_show=len(show)
        days=np.arange(1,len_show+1)

        return days,show

    def standard_show_Kelly_Criterion(self,changer,first_base_income=1000000,day_interval=2,label="自己"):
        Kelly_rate=0.90
        start_from=first_base_income
        show=[]
        for curchange in changer:
            start_from=start_from*(1+curchange/100/day_interval-0.0020/day_interval)*Kelly_rate+start_from*(1-Kelly_rate)
            show.append(start_from)

        #print(show)
        len_show=len(show)
        days=np.arange(1,len_show+1)

        #fig=plt.figure(figsize=(6,3))


        #plt.show()

        return days,show

    def show_today(self):

        show=pd.read_csv('out1.csv',index_col=0,header=0)
        show2=pd.read_csv('out2.csv',index_col=0,header=0)
        show3=pd.read_csv('out3.csv',index_col=0,header=0)
        show4=pd.read_csv('out4.csv',index_col=0,header=0)

        show['9']=show['9']+show2['9']+show3['9']+show4['9']
        show['mix']=show['mix']+show2['mix']+show3['mix']+show4['mix']


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

        b.to_csv("today_real_remix_result.csv")

        fsfef=1