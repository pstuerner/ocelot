import requests
import re
from bs4 import BeautifulSoup
from ocelot.scrape.base import Scraper
from datetime import datetime as dt

class CIKScraper(Scraper):

    base_url = """https://www.sec.gov/cgi-bin/browse-edgar?action=getcurrent&datea=&dateb=&company=&type=13f-hr&SIC=&State=&Country=&CIK=&owner=include&accno=&start={start}&count={count}"""

    def __init__(self):
        super().__init__()
        
    @staticmethod
    def scrape(ciks, s, c):
        soup = BeautifulSoup(requests.get(CIKScraper.base_url.format(start=s, count=c)).text, "html.parser")
        a_tags = [x['href'] for x in soup.find_all("a", href=re.compile("CIK="))[1:]]
        for a_tag in a_tags:
            ciks.append(a_tag[a_tag.find("CIK=")+4:a_tag.find("&owner")])
        
        button_tag = soup.find('input', value=f'Next {c}')
        if button_tag != None:
            return CIKScraper.scrape(ciks, s+c, c)
        
        return ciks
        
    def write(self, ciks):
        i = 0
        for cik in ciks:
            insert = self.db.cik_storage.update({'cik':cik},{'$set':{'cik':cik,'timestamp':dt.now()}}, upsert=True)
            if insert['updatedExisting'] == False:
                i += 1
        
        print(f'ocelot has written {i} new CIK(s) into the database.')