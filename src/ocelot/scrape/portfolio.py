import requests
import re
import xml.etree.ElementTree as ET
import pandas as pd
import os
import logging
from bs4 import BeautifulSoup
from ocelot.temp import path
from ocelot.utils import type_mapping, db

class Portfolio:

    col_portfolios = db.portfolios
    
    def __init__(self, cik, acc_no, dt_period, dt_effective, url_xml):
        self.logger = logging.getLogger('ocelot.portfolio.Portfolio')
        self.logger.info('Creating an instance of Portfolio')
        self.cik = cik
        self.acc_no = acc_no
        self.dt_period = dt_period
        self.dt_effective = dt_effective
        self.url_xml = url_xml
    
    @staticmethod
    def download_xml(url):
        r = requests.get(url, stream=True)
        with open(os.path.join(path,"temp.xml"), 'wb') as f:
            for chunk in r.iter_content(chunk_size=1024):
                if chunk:
                    f.write(chunk)
                    f.flush()
    
    @staticmethod
    def read_xml():
        it = ET.iterparse(os.path.join(path,"temp.xml"))
        for _, el in it:
            prefix, has_namespace, postfix = el.tag.partition('}')
            if has_namespace:
                el.tag = postfix

        return it.root
    
    def scrape(self):
        self.logger.info(f'Scraping {self.acc_no}')
        Portfolio.download_xml(self.url_xml)
        info_tables = Portfolio.read_xml().findall("infoTable")
        portfolio = []
        for info_table in info_tables:
            portfolio.append(
                {x.tag.lower(): x.text for x in info_table.iter() if (x.tag != "infoTable" and str(x.text)[:1] != "\n")}
            )
        df = pd.DataFrame(portfolio)
        for col in df.columns:
            if col not in list(type_mapping.keys()):
                df.drop(columns=col, inplace=True)
            else:
                df[col] = df[col].astype(type_mapping[col])

        self.df = df
        q = db[self.acc_no].insert_many(df.to_dict("records"))
        self.logger.info(f'Scraped {len(df)} positions from {self.acc_no}')

        return q
    
    def insert(self, status):
        q = Portfolio.col_portfolios.insert_one(
                {
                    'cik': self.cik,
                    'acc_no': self.acc_no,
                    'dt_period': self.dt_period,
                    'dt_effective': self.dt_effective,
                    'url_xml': self.url_xml,
                    'status': status
                }
            )

        return q
    
    @staticmethod
    def update(acc_no, status):
        q = Portfolio.col_portfolios.update_one(
                {
                    'acc_no': acc_no
                },
                {
                    '$set': {'status': status}
                }
            )
        return q
    
    @staticmethod
    def delete(acc_no):
        q = db[acc_no].drop()

        return q
    