#coding=utf-8

class Display(object):
    """description of class"""

    def scatter(self):
        showsource=pd.read_csv('data2020mixd.csv',index_col=0,header=0)
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

    def plotall(self):

        #days,show1,show2=get_allchange()


        #get_codeanddate_feature()

        #feature_env_codeanddate3('2017')

        #lgb_train_2('2017')

        days,show3=self.show_all_rate()

        #plt.plot(days,show1,c='blue',label="000001")
        #plt.plot(days,show2,c='red',label="399006")
        plt.plot(days,show3,c='green',label="my model head10mean")

        plt.legend()

        plt.show()

    def show_all_rate(self):
        showsource=pd.read_csv('data2020mixd.csv',index_col=0,header=0)
        databuffer=showsource['trade_date'].unique()

        changer=[]
        for curdata in databuffer:

            cur_show=showsource[showsource["trade_date"]==curdata]
            b=cur_show.sort_values(by="mix" , ascending=False)
            #b=cur_show.sort_values(by="9" , ascending=True)
            #d=b.head(10)
            #e=d.sort_values(by="mix" , ascending=True)
        

            #b=cur_show[cur_show['mix']>0.40]
            average=b.head(10)['tomorrow_chg'].mean()
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