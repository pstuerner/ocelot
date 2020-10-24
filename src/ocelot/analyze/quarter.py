import pandas as pd
import numpy as np
from tqdm import tqdm
from ocelot.analyze.portfolio import Portfolio
from ocelot.utils import db

class Quarter:

    def __init__(self, dt_period):
        self.dt_period = dt_period
        self.portfolios = list(db['portfolios'].find({'dt_period':dt_period,'status':'scraped'}))
    
    def snapshot(self):
        portfolios = (Portfolio(p['acc_no']).positions for p in self.portfolios)
        
        df = next(portfolios).assign(cnt=1)
        for portfolio in tqdm(portfolios, total=len(self.portfolios)-1):
            df = (
                (pd.concat([df,portfolio.assign(cnt=1)]))
                .groupby('cusip')
                .agg(
                    nameofissuer=pd.NamedAgg(column='nameofissuer', aggfunc='first'),
                    titleofclass=pd.NamedAgg(column='titleofclass', aggfunc='first'),
                    value=pd.NamedAgg(column='value', aggfunc='sum'),
                    sshprnamt=pd.NamedAgg(column='sshprnamt', aggfunc='sum'),
                    cnt=pd.NamedAgg(column='cnt', aggfunc='sum'),
                    )
                .reset_index()
                )
        
        self.snapshot = df
        
        return df

    @staticmethod
    def portfolio_change(p_prev, p_current):
        df = (
        pd
        .merge(p_prev.positions,p_current.positions,on='cusip',how='outer',indicator=True)
        .assign(
                value_x=lambda df: df.value_x.fillna(0),
                value_y=lambda df: df.value_y.fillna(0),
                sshprnamt_x=lambda df: df.sshprnamt_x.fillna(0),
                sshprnamt_y=lambda df: df.sshprnamt_y.fillna(0),
                sshprnamt=lambda df: df.sshprnamt_y-df.sshprnamt_x,
                value=lambda df: df.value_y-df.value_x,
                nameofissuer=lambda df: np.where(df.nameofissuer_x.isna(),df.nameofissuer_y,df.nameofissuer_x),
                titleofclass=lambda df: np.where(df.titleofclass_x.isna(),df.titleofclass_y,df.titleofclass_x),
                increase=lambda df:(np.logical_and(df._merge=='both',df.sshprnamt>0))*1,
                decrease=lambda df:(np.logical_and(df._merge=='both',df.sshprnamt<0))*1,
                unchange=lambda df:(np.logical_and(df._merge=='both',df.sshprnamt==0))*1,
                enter=lambda df:(df._merge=='right_only')*1,
                leave=lambda df:(df._merge=='left_only')*1,
        )
        [['cusip','nameofissuer','titleofclass','value','sshprnamt','increase','decrease','unchange','enter','leave']]
        )

        return df

    def change(self, dt_other):
        """
        I = Increase
        D = Decrease
        U = Unchange
        E = Enter
        L = Leave
        """

        all_portfolios = []
        col_name = f"{dt_other.strftime('%Y-%m-%d')}to{self.dt_period.strftime('%Y-%m-%d')}"
        for p_current in self.portfolios:
            p_prev = db.portfolios.find_one({'cik':p_current['cik'],'status':'scraped','dt_period':dt_other})
            if p_prev is not None:
                all_portfolios.append((p_current['cik'], p_current['acc_no'], p_prev['acc_no']))
        
        for cik,p_current,p_prev in tqdm(all_portfolios):
            pc = Quarter.portfolio_change(Portfolio(p_current), Portfolio(p_prev))
            try:
                db[col_name].insert_many(pc.to_dict("records"))
            except Exception as e:
                pass
            
        df = (
            pd
            .DataFrame(db[col_name].find({},{'_id':0}))
            .groupby(['cusip','increase','decrease','unchange','enter','leave'])
            .agg(
                nameofissuer=pd.NamedAgg(column='nameofissuer', aggfunc='first'),
                titleofclass=pd.NamedAgg(column='titleofclass', aggfunc='first'),
                value=pd.NamedAgg(column='value', aggfunc='sum'),
                sshprnamt=pd.NamedAgg(column='sshprnamt', aggfunc='sum'),
                cnt=pd.NamedAgg(column='nameofissuer', aggfunc='count'),
                )
            .reset_index()
            )
        
        self.change = df

        return df



        
