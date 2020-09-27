from ocelot.utils import db
from ocelot.analyze.portfolio import Portfolio

class AssetManager:

    def __init__(self, cik):
        self.cik = cik
        self.info = db['assetmanagers'].find_one({'cik': cik})
        self.portfolios = AssetManager.all_portfolios(cik)

    @staticmethod
    def all_portfolios(cik):
        portfolios = db['portfolios'].find({'cik':cik,'status':'scraped'}).sort('dt_period', -1)

        return list(portfolios)

    def get_portfolio(self, idx=None, acc_no=None, dt_period=None):
        assert len([x for x in [idx, acc_no, dt_period] if x==None])==2, 'Exactly one of idx, acc_no, dt_period must be != None'

        if idx!=None:
            return Portfolio(acc_no=self.portfolios[idx]['acc_no'])
        elif acc_no!=None:
            return Portfolio(acc_no=acc_no)
        else:
            return Portfolio(acc_no=next(p for p in self.portfolios if p['dt_period']==dt_period)['acc_no'])