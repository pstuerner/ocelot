import re
import requests
from bs4 import BeautifulSoup
from datetime import datetime as dt
from ocelot.scrape.base import Scraper

class AMScraper(Scraper):

    base_url = 'https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&CIK={cik}&type=13f-hr%25&dateb=&owner=include&start={start}&count={count}'

    def __init__(self, cik):
        super().__init__()
        self.cik = cik
        self.reports = []
    
    @staticmethod
    def scrape_all_report_urls(report_urls, cik, s, c):
        soup = BeautifulSoup(requests.get(AMScraper.base_url.format(cik=cik, start=s, count=c)).text, "html.parser")
        tr_tags = soup.find_all(lambda tag: tag.name=='tr' and tag.find('a', id=True)!=None)
        urls = [tr.find_all('td')[1].find('a')['href'] for tr in tr_tags]
        for url in urls:
            report_urls.append(f'https://www.sec.gov{url}')
        
        button_tag = soup.find('input', value=f'Next {c}')
        if button_tag != None:
            return AMScraper.scrape_all_report_urls(report_urls, cik, s+c, c)
        
        return report_urls
    
    def scrape_all_relevant_reports(self):
        col = self.db.all_reports
        report_urls = AMScraper.scrape_all_report_urls([], self.cik, 0, 100)

        for report_url in report_urls[:1]:
            soup = BeautifulSoup(requests.get(report_url).text, "html.parser")
            acc_no = re.findall(r'\d+-\d+-\d+', soup.find('div', id='secNum').text)[0]
            dt_period = dt.strptime(soup.find_all('div', class_='info')[-2].text, '%Y-%m-%d')
            dt_effective = dt.strptime(soup.find_all('div', class_='info')[-1].text, '%Y-%m-%d')
            url_xml = f"https://www.sec.gov{soup.find_all('a', href=re.compile('.xml'))[-1]['href']}"

            q = col.find({'cik':self.cik,'dt_period':dt_period})
            if q.count() > 0:
                report_newest = sorted(q, key=lambda x: x['dt_effective'], reverse=True)[0]
                if dt_effective > report_newest['dt_effective']:
                    col.update_one(
                        {
                            'cik': self.cik,
                            'dt_period': report_newest['dt_period'],
                            'dt_effective': report_newest['dt_effective'],
                        },
                        {
                            '$set': {'status': 'ignore'}
                        }
                    )
                    col.insert_one(
                        {
                            'cik': self.cik,
                            'dt_period': dt_period,
                            'dt_effective': dt_effective,
                            'url_xml': url_xml,
                            'status': 'pending'
                        }
                    )
                    # TODO: delete unamended portfolio
                else:
                    col.insert_one(
                        {
                            'cik': self.cik,
                            'dt_period': dt_period,
                            'dt_effective': dt_effective,
                            'url_xml': url_xml,
                            'status': 'ignore'
                        }
                    )
            else:
                col.insert_one(
                    {
                        'cik': self.cik,
                        'dt_period': dt_period,
                        'dt_effective': dt_effective,
                        'url_xml': url_xml,
                        'status': 'pending'
                    }
                )

        return
    
