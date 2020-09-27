import pandas as pd
from ocelot.utils import db

class Portfolio:

    def __init__(self, acc_no):
        self.info = db['portfolios'].find_one({'acc_no': acc_no})
        self.acc_no = acc_no
        self.positions = Portfolio.positions(acc_no)

    @staticmethod
    def positions(acc_no, columns=['nameofissuer', 'titleofclass', 'cusip', 'value', 'sshprnamt', 'putcall']):
        d = {'_id':0}
        for column in columns:
            d[column] = 1
        return pd.DataFrame(list(db[acc_no].find({},d)))
    
    
    
