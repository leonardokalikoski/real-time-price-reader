from bs4 import BeautifulSoup
import requests
import urllib.request
import pandas as pd
from datetime import datetime
import time
from dotenv import load_dotenv
import os

load_dotenv()
URL_CORE = os.getenv('URL')
WSL_PATH = os.getenv('WSL_PATH')
print('Environment Variables Loaded')

class Scraper:
    def __init__(self):
        self.results = []

    def get_html(self, stock_symbol, stock_type):
        url = f"{URL_CORE}{stock_type}/{stock_symbol}"
        headers = requests.utils.default_headers()
        headers.update({
            'User-Agent': 'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:52.0) Gecko/20100101 Firefox/52.0',"cookie": "CONSENT=YES+cb.20230531-04-p0.en+FX+908"})
        try:
            response = requests.get(url, headers=headers)
        except requests.exceptions.RequestException as e:
            raise Exception(f"Bad request for URL {url}: {e}")
        html_doc = response.text
        return html_doc


    def get_price(self, html_doc):
        soup = BeautifulSoup(html_doc, 'html.parser')
        try:
            all_price_elements = soup.find('div', class_ = 'info special w-100 w-md-33 w-lg-20')
            if not all_price_elements:
                raise ValueError('Price Class "info special w-100 w-md-33 w-lg-20" not found')
        except Exception as e:
            print(f"Error exception: {e}")

        try:
            price = all_price_elements.find('strong', class_ = 'value')
            if not price:
                raise ValueError('Price class "_value" not found')
        except Exception as e:
            print(f"Error Exception {e}")
        if price:
            adjusted_price = price.text.strip() # Adjusts br number type to us
        else:
            adjusted_price = 0
        return adjusted_price
    
    def add_to_csv(self, data):
        df = pd.DataFrame(data)
        path = WSL_PATH
        df.to_csv(path + 'prices.csv', mode='w', header=True, index=False) # write mode replaces table

    
    def scrape(self, stock_dictionary):
        self.results.clear()  # Clear old results before scraping new data
        for selected_key_pair in stock_dictionary.items(): # iterates over the dictionary
            symbol, stock_type = selected_key_pair # splits keypard into symbol + stock_type (selected_key_pair = 'HGRE11': 'fundos-imobiliarios')
            #print(scraper) # for debugging
            html = self.get_html(symbol, stock_type) # adds the symbol and type to the URL
            price = self.get_price(html)
            price = price.replace(",",".")
            today = datetime.today().strftime('%Y-%m-%d %H:%M:%S')

            print(f'Processing {symbol} at {price} at date {today}')
            self.results.append({'Symbol': symbol , 'Price': price, 'Date': today})
        self.add_to_csv(self.results)


def main():
    stock_dict = {
    'HGRE11': 'fundos-imobiliarios',
    'HFOF11': 'fundos-imobiliarios',
    'KNCA11': 'fiagros',
    'RBRR11': 'fundos-imobiliarios',
    'SNCI11': 'fundos-imobiliarios',
    'HGBS11': 'fundos-imobiliarios',
    'BTLG11': 'fundos-imobiliarios',
    'URPR11': 'fundos-imobiliarios',
    'RZAK11': 'fundos-imobiliarios',
    'JSRE11': 'fundos-imobiliarios',
    'KNRI11': 'fundos-imobiliarios',
    'RECR11': 'fundos-imobiliarios',
    'KNCR11': 'fundos-imobiliarios',
    'RZTR11': 'fundos-imobiliarios',
    'XPML11': 'fundos-imobiliarios',
    'HGRU11': 'fundos-imobiliarios',
    'BRCO11': 'fundos-imobiliarios'
}
    
    scraper = Scraper()
    while True:
        scraper.scrape(stock_dict)
        time.sleep(1)


if __name__ == '__main__':
    main()
