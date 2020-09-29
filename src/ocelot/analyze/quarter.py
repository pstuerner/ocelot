import pandas as pd
from ocelot.analyze.portfolio import Portfolio
from ocelot.utils import db

class Quarter:

    def __init__(self, dt_period):
        self.dt_period = dt_period
        self.portfolios = list(db['portfolios'].find({'dt_period':dt_period,'status':'scraped'}))
    
    def positions(self):
        portfolios = (Portfolio(p['acc_no']).positions for p in self.portfolios)
        
        df = next(portfolios)
        for portfolio in portfolios:
            df = (
                (pd.concat([df,portfolio]))
                .groupby('cusip')
                .agg(
                    nameofissuer=pd.NamedAgg(column='nameofissuer', aggfunc='first'),
                    titleofclass=pd.NamedAgg(column='titleofclass', aggfunc='first'),
                    value=pd.NamedAgg(column='value', aggfunc='sum'),
                    sshprnamt=pd.NamedAgg(column='sshprnamt', aggfunc='sum'),
                    )
                .reset_index()
                )
        
        return df

        
