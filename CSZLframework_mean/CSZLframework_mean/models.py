#coding=utf-8
import abc
import pandas as pd
import numpy as np

import lightgbm as lgb
from sklearn import datasets
from sklearn.model_selection import StratifiedKFold
from sklearn.externals import joblib
import os

class BaseModel(object):
    """description of class"""

    def __init__(self):
        pass

    #@abc.abstractmethod
    def predict(self, Predictpath,modelpath):
        """Subclass must implement this."""
        #print (self.__class__.__name__)
        (filepath, tempfilename) = os.path.split(Predictpath)
        (filename, extension) = os.path.splitext(tempfilename)

        (filepath2, tempfilename2) = os.path.split(modelpath)
        (filename2, extension2) = os.path.splitext(tempfilename2)
        


        bufferstringoutput=filepath+'/'+filename+'_'+filename2+'.csv'

        if(os.path.exists(bufferstringoutput)==False):    

            df_all=pd.read_csv(Predictpath,index_col=0,header=0)
            #df_all=pd.read_csv(bufferstring,index_col=0,header=0,nrows=100000)
            self.core_predict(df_all,modelpath,bufferstringoutput)
            #df_all.to_csv(bufferstringoutput)

        return bufferstringoutput

    def train(self,DataSetName):
        #print (self.__class__.__name__)
        (filepath, tempfilename) = os.path.split(DataSetName)
        (filename, extension) = os.path.splitext(tempfilename)

        bufferstringoutput=filepath+'/'+filename+'_'+self.__class__.__name__+'.pkl'

        if(os.path.exists(bufferstringoutput)==False):    

            df_all=pd.read_csv(DataSetName,index_col=0,header=0)
            #df_all=pd.read_csv(bufferstring,index_col=0,header=0,nrows=100000)
            self.core_train(df_all,bufferstringoutput)
            #df_all.to_csv(bufferstringoutput)

        return bufferstringoutput


    def core_train(self, dataset,savepath):
        pass

    def core_predict(self,dataset,modelpath,savepath):
        pass

class LGBmodel(BaseModel):


    def core_predict(self,train,modelpath,savepath):
        """Subclass must implement this."""

        #readstring='ztrain'+year+'.csv'

        #train=pd.read_csv(readstring,index_col=0,header=0,nrows=10000)
        #train=pd.read_csv(readstring,index_col=0,header=0)
        train=train.reset_index(drop=True)
        train2=train.copy(deep=True)


        y_train = np.array(train['tomorrow_chg_rank'])
        train.drop(['tomorrow_chg','tomorrow_chg_rank','ts_code','trade_date'],axis=1,inplace=True)

        #corrmat = train.corr()
        #f, ax = plt.subplots(figsize=(12, 9))
        #sns.heatmap(corrmat, vmax=.8, square=True);
        #plt.show()

        lgb_model = joblib.load(modelpath)

        dsadwd=lgb_model.feature_importances_

        pred_test = lgb_model.predict_proba(train)

        data1 = pd.DataFrame(pred_test)

        data1['mix']=0
        #multlist=[-12,-5,-3,-2,-1.5,-1,-0.75,-0.5,-0.25,0,0,0.25,0.5,0.75,1,1.5,2,3,5,12]
        multlist=[-10,-3,-2,-1,0,0,1,2,3,10]

        for i in range(10):
            buffer=data1[i]*multlist[i]
            data1['mix']=data1['mix']+buffer

        train2=train2.join(data1)
    
        print(train2)

        train2.to_csv(savepath)

        return 2

    def core_train(self,train,savepath):

        #readstring='ztrain'+year+'.csv'
        
        #readstring=path+'.csv'

        ##train=pd.read_csv(readstring,index_col=0,header=0,nrows=10000)
        #train=pd.read_csv(readstring,index_col=0,header=0)
        train=train.reset_index(drop=True)
        train2=train.copy(deep=True)

    

        y_train = np.array(train['tomorrow_chg_rank'])
        train.drop(['tomorrow_chg','tomorrow_chg_rank','ts_code','trade_date'],axis=1,inplace=True)


        train_ids = train.index.tolist()

        skf = StratifiedKFold(n_splits=5, shuffle=True, random_state=22)
        skf.get_n_splits(train_ids, y_train)

        train=train.values

        counter=0

        for train_index, test_index in skf.split(train_ids, y_train):
        
            X_fit, X_val = train[train_index],train[test_index]
            y_fit, y_val = y_train[train_index], y_train[test_index]

            lgb_model = lgb.LGBMClassifier(max_depth=-1,
                                           n_estimators=400,
                                           learning_rate=0.05,
                                           num_leaves=2**8-1,
                                           colsample_bytree=0.6,
                                           objective='multiclass', 
                                           num_class=21,
                                           n_jobs=-1)
                                   

            lgb_model.fit(X_fit, y_fit, eval_metric='multi_error',
                          eval_set=[(X_val, y_val)], 
                          verbose=100, early_stopping_rounds=100)
        
            joblib.dump(lgb_model,savepath)
            break

            lgb_model = joblib.load(savepath)

            pred_test = lgb_model.predict_proba(X_val)

            #np.set_printoptions(threshold=np.inf) 

            #pd.set_option('display.max_rows', 10000)  # 设置显示最大行
            #pd.set_option('display.max_columns', None)
            #print(pred_test)

            data1 = pd.DataFrame(pred_test)
            data1.to_csv('data1.csv')

            gc.collect()
            counter += 1    
            #Stop fitting to prevent time limit error
            if counter == 1 : break


        #X_train,X_test,y_train,y_test=train_test_split(iris.data,iris.target,test_size=0.3)        

        return 2

        #return super().train(dataset)

    def real_lgb_predict(self,modelpath):
        readstring='today_train.csv'

        #train=pd.read_csv(readstring,index_col=0,header=0,nrows=10000)
        train=pd.read_csv(readstring,index_col=0,header=0)
        train=train.reset_index(drop=True)
        train2=train.copy(deep=True)

        #train.drop(['tomorrow_chg','tomorrow_chg_rank','ts_code','trade_date'],axis=1,inplace=True)
        train.drop(['ts_code','trade_date'],axis=1,inplace=True)


        lgb_model = joblib.load(modelpath)

        dsadwd=lgb_model.feature_importances_

        pred_test = lgb_model.predict_proba(train)

        data1 = pd.DataFrame(pred_test)

        data1['mix']=0
        multlist=[-10,-3,-2,-1,0,0,1,2,3,10]

        for i in range(10):
            buffer=data1[i]*multlist[i]
            data1['mix']=data1['mix']+buffer

        train2=train2.join(data1)
    
        print(train2)
        readstring='todaypredict.csv'
        train2.to_csv(readstring)

        dawd=1


class RFmodel(BaseModel):


    def predict(self, dataset):
        """Subclass must implement this."""
        return 2
