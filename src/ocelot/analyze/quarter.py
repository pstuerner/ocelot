import pandas as pd
from ocelot.analyze.portfolio import Portfolio
from ocelot.utils import db

class Quarter:

    def __init__(self, dt_period):
        self.dt_period = dt_period
        self.portfolios = list(db['portfolios'].find({'dt_period':dt_period,'status':'scraped'}))
    
    def positions(self):
        portfolios = (Portfolio(p['acc_no']).positions for p in self.portfolios)
        df_parent = next(portfolios)

        for portfolio in portfolios:
            df_child = portfolio
            display(df_parent)
            display(df_child)
            raise ValueError
