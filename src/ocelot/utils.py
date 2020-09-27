import requests
import re
from pymongo import MongoClient
from bs4 import BeautifulSoup
from ocelot import config

latest_filings_url = """https://www.sec.gov/cgi-bin/browse-edgar?action=getcurrent&datea=&dateb=&company=&type=13f-hr&SIC=&State=&Country=&CIK=&owner=include&accno=&start={start}&count={count}"""
cik_brk = '0001067983' # berkshire hathaway cik for testing purposes, company has lots of reports
cik_2s = '0001478735' # twosigma cik for testing purposes

mc = MongoClient(
    username = config['mongodb']['username'],
    password = config['mongodb']['password'],
    authSource = config['mongodb']['authsource']
)
db = mc[config['mongodb']['database']]

type_mapping = {
    "nameofissuer": str,
    "titleofclass": str,
    "cusip": str,
    "sshprnamttype": str,
    "investmentdiscretion": str,
    "putcall": str,
    "value": float,
    "sshprnamt": float,
    "sole": float,
    "shared": float,
    "none": float,
    "othermanager": str
}

def scrape_latest_filings(filings=[], s=0, c=100):
    soup = BeautifulSoup(requests.get(latest_filings_url.format(start=s, count=c)).text, "html.parser")
    tr_tags = soup.find_all(lambda tag: tag.name=='tr' and tag.find('td', class_='small'))
    a_tags = soup.find_all('a', href=re.compile('getcompany&CIK'))
    for tr_tag, a_tag in zip(tr_tags, a_tags):
        portfolio_url = f"https://www.sec.gov{tr_tag.find('a')['href']}"
        acc_no = re.findall(r'\d+-\d+-\d+', portfolio_url)[0]
        cik = re.findall(r'=\d+&', a_tag['href'])[0][1:-1]
        filings.append(
            {
                'portfolio_url': portfolio_url,
                'acc_no': acc_no,
                'cik': cik
            }
        )
    
    button_tag = soup.find('input', value=f'Next {c}')
    if button_tag != None:
        return scrape_latest_filings(filings, s+c, c)
    
    return filings

def delete_all_collections(really=False):
    if not really:
        return
    
    cols = db.list_collection_names()
    for col in cols:
        db[col].drop()