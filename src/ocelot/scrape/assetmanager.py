import re
import requests
import logging
from bs4 import BeautifulSoup
from datetime import datetime as dt
from ocelot.utils import db
from ocelot.scrape.portfolio import Portfolio

class AssetManager():

    col_assetmanagers = db.assetmanagers
    col_portfolios = db.portfolios

    def __init__(self, cik, acc_no):
        self.logger = logging.getLogger('ocelot.assetmanager.AssetManager')
        self.logger.info('Creating an instance of AssetManager')
        self.cik = cik
        self.acc_no = acc_no
        self.url = 'https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&CIK={cik}&type=13f-hr%25&dateb=&owner=include&start={start}&count={count}'
        self.reports = []
    
    def all_portfolio_urls(self, portfolio_urls, cik, s, c):
        soup = BeautifulSoup(requests.get(self.url.format(cik=cik, start=s, count=c)).text, "html.parser")
        tr_tags = soup.find_all(lambda tag: tag.name=='tr' and tag.find('a', id=True)!=None)
        urls = [tr.find_all('td')[1].find('a')['href'] for tr in tr_tags]
        dates = [tr.find_all('td')[3].text for tr in tr_tags]
        for url, date in zip(urls, dates):
            portfolio_urls.append((f'https://www.sec.gov{url}', dt.strptime(date, '%Y-%m-%d')))
        
        button_tag = soup.find('input', value=f'Next {c}')
        if button_tag != None:
            return self.all_portfolio_urls(portfolio_urls, cik, s+c, c)
        
        cname_tag = soup.find('span', class_='companyName').text
        self.company_name = cname_tag[:cname_tag.find('CIK#')-1]
        
        return portfolio_urls
    
    def scrape(self, portfolio_urls=[]):
        q = AssetManager.col_assetmanagers.find_one({'cik':self.cik})
        if q != None:
            q = AssetManager.col_portfolios.find_one({'acc_no':self.acc_no})
            if q != None:
                self.logger.info(f'Skipping {self.acc_no}: Already exists')
                return {'new':0, 'updated':0}
            else:
                new = 0
                updated = 1
                self.company_name = q['name']
        else:
            portfolio_urls = self.all_portfolio_urls([], self.cik, 0, 100)
            new = updated = 1
        self.logger.info(f'Scraping {len(portfolio_urls)} portfolio(s) for {self.company_name}')

        for portfolio_url in portfolio_urls:
            if portfolio_url[1] < dt(2013, 6, 30):
                self.logger.info(f'Skipping {portfolio_url[0]}: Period of report before 2013/06/30')
                continue

            soup = BeautifulSoup(requests.get(portfolio_url[0]).text, "html.parser")
            acc_no = re.findall(r'\d+-\d+-\d+', soup.find('div', id='secNum').text)[0]
            dt_period = dt.strptime(soup.find_all('div', class_='info')[-2].text, '%Y-%m-%d')
            dt_effective = dt.strptime(soup.find_all('div', class_='info')[-1].text, '%Y-%m-%d')
            
            if dt_period < dt(2013, 6, 30): # no uniform xml format before 2013/06/30
                self.logger.info(f'Skipping {acc_no}: Period of report before 2013/06/30')
                continue
            
            url_xml = f"https://www.sec.gov{soup.find_all('a', href=re.compile('.xml'))[-1]['href']}"
            p = Portfolio(self.cik, acc_no, dt_period, dt_effective, url_xml)

            q = AssetManager.col_portfolios.find({'cik':self.cik, 'dt_period':dt_period})
            if q.count() > 0:
                portfolio_newest = sorted(q, key=lambda x: x['dt_effective'], reverse=True)[0]
                if dt_effective > portfolio_newest['dt_effective']:
                    self.logger.info(f'Inserting {acc_no} and deleting {portfolio_newest["acc_no"]}: New amendment')
                    p.delete(portfolio_newest['acc_no'])
                    p.update(portfolio_newest['acc_no'], 'ignored_a')
                    p.scrape()
                    p.insert(status='scraped')
                else:
                    self.logger.info(f'Inserting {acc_no} with status "ignored_a": Existing amendment')
                    p.insert(status='ignored_a')
            else:
                self.logger.info(f'Inserting {acc_no}: New portfolio')
                p.scrape()
                p.insert(status='scraped')

        return {'new': new, 'updated': updated}
    
    def insert(self):
        q = AssetManager.col_assetmanagers.insert_one(
            {
                'cik': self.cik,
                'company_name': self.company_name,
                'url': self.url.format(cik=self.cik, start=0, count=100),
                'last_update': dt.now()
            }
        )

        return q
    
    def update(self):
        q = AssetManager.col_assetmanagers.update_one(
            {'cik': self.cik},
            {'$set': {'last_update':dt.now()}}
        )

        return q
    
