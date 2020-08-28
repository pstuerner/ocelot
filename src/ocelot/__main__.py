import argparse
from ocelot import config
from ocelot.scrape.cik import CIKScraper

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
    scraper = CIKScraper()
    ciks = scraper.scrape([], 0, 40)
    scraper.write(ciks)

if __name__ == "__main__":
    main()
    