import pandas as pd
from ocelot.utils import db

class Portfolio:

    def __init__(self, acc_no):
        self.info = db['portfolios'].find_one({'acc_no': acc_no})
        self.acc_no = acc_no
        self.positions = Portfolio.positions(acc_no)

    @staticmethod
    def positions(acc_no, putcall='ignore', agg=True):
        cols = {'nameofissuer':1, 'titleofclass':1, 'cusip':1, 'value':1, 'sshprnamt':1, 'putcall':1}
        ps = (pd.DataFrame(list(db[acc_no].find({},cols))).assign(cusip=lambda df:df.cusip.str.upper()))

        if putcall == 'ignore':
            if 'putcall' in ps.columns:
                ps = ps[lambda df:~df.putcall.str.lower().str.strip().isin(['put','call'])]
        
        if agg:
            ps = (ps.groupby('cusip').agg(
                    nameofissuer=pd.NamedAgg(column='nameofissuer', aggfunc='first'),
                    titleofclass=pd.NamedAgg(column='titleofclass', aggfunc='first'),
                    value=pd.NamedAgg(column='value', aggfunc='sum'),
                    sshprnamt=pd.NamedAgg(column='sshprnamt', aggfunc='sum'),
                    ).reset_index())

        return ps
