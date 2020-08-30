import argparse
import logging
from datetime import datetime as dt
from ocelot import config
from ocelot.utils import scrape_latest_filings
from ocelot.scrape.assetmanager import AssetManager

logger = logging.getLogger('ocelot')
logger.setLevel(logging.DEBUG)
fh = logging.FileHandler('ocelot.log')
fh.setLevel(logging.DEBUG)
sh = logging.StreamHandler()
sh.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
fh.setFormatter(formatter)
sh.setFormatter(formatter)
logger.addHandler(fh)
logger.addHandler(sh)

def main():
    parser = argparse.ArgumentParser()
    subparser = parser.add_subparsers()
    subparser.required = True

    scrape_parser = subparser.add_parser('scrape')
    scrape_parser.set_defaults(cmd='scrape')

    params = parser.parse_args()

    if params.cmd == 'scrape':
        scrape()

def scrape():
    logger.info(f'Scraping latest filings')
    latest_filings = scrape_latest_filings()
    logger.info(f'Found {len(latest_filings)} possible filings')
    for filing in latest_filings:
        am = AssetManager(
            cik=filing['cik'],
            acc_no=filing['acc_no']
        )
        result = am.scrape(portfolio_urls=[(filing['portfolio_url'], dt.now())])
        if result['new'] == 1:
            am.insert()
        else:
            if result['updated'] == 1:
                am.update()
            else:
                pass


if __name__ == "__main__":
    

    main()
    