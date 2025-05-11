import requests
import random
import time
from bs4 import BeautifulSoup
import pandas as pd
import numpy as np

class Finhandler():
    def __init__(self, n_retries=16):
        self.base_url = "https://www.finanzen.net/"
        self.search_url = "https://www.finanzen.net/suchergebnis.asp?_search="
        self.n_retries = n_retries
        self.user_agent_list = [
            "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.1 (KHTML, like Gecko) Chrome/22.0.1207.1 Safari/537.1",
            "Mozilla/5.0 (X11; CrOS i686 2268.111.0) AppleWebKit/536.11 (KHTML, like Gecko) Chrome/20.0.1132.57 Safari/536.11",
            "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/536.6 (KHTML, like Gecko) Chrome/20.0.1092.0 Safari/536.6",
            "Mozilla/5.0 (Windows NT 6.2) AppleWebKit/536.6 (KHTML, like Gecko) Chrome/20.0.1090.0 Safari/536.6",
            "Mozilla/5.0 (Windows NT 6.2; WOW64) AppleWebKit/537.1 (KHTML, like Gecko) Chrome/19.77.34.5 Safari/537.1",
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/536.5 (KHTML, like Gecko) Chrome/19.0.1084.9 Safari/536.5",
            "Mozilla/5.0 (Windows NT 6.0) AppleWebKit/536.5 (KHTML, like Gecko) Chrome/19.0.1084.36 Safari/536.5",
            "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/536.3 (KHTML, like Gecko) Chrome/19.0.1063.0 Safari/536.3",
            "Mozilla/5.0 (Windows NT 5.1) AppleWebKit/536.3 (KHTML, like Gecko) Chrome/19.0.1063.0 Safari/536.3",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_8_0) AppleWebKit/536.3 (KHTML, like Gecko) Chrome/19.0.1063.0 Safari/536.3",
            "Mozilla/5.0 (Windows NT 6.2) AppleWebKit/536.3 (KHTML, like Gecko) Chrome/19.0.1062.0 Safari/536.3",
            "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/536.3 (KHTML, like Gecko) Chrome/19.0.1062.0 Safari/536.3",
            "Mozilla/5.0 (Windows NT 6.2) AppleWebKit/536.3 (KHTML, like Gecko) Chrome/19.0.1061.1 Safari/536.3",
            "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/536.3 (KHTML, like Gecko) Chrome/19.0.1061.1 Safari/536.3",
            "Mozilla/5.0 (Windows NT 6.1) AppleWebKit/536.3 (KHTML, like Gecko) Chrome/19.0.1061.1 Safari/536.3",
            "Mozilla/5.0 (Windows NT 6.2) AppleWebKit/536.3 (KHTML, like Gecko) Chrome/19.0.1061.0 Safari/536.3",
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/535.24 (KHTML, like Gecko) Chrome/19.0.1055.1 Safari/535.24",
            "Mozilla/5.0 (Windows NT 6.2; WOW64) AppleWebKit/535.24 (KHTML, like Gecko) Chrome/19.0.1055.1 Safari/535.24",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/86.0.4240.111 Safari/537.36",
            "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:135.0) Gecko/20100101 Firefox/135.0"
        ]
        self.headers = {
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Encoding": "gzip, deflate, br, zstd",
            "Accept-Language": "en-US,en;q=0.5",
            "Connection": "keep-alive",
            "Sec-Fetch-Dest": "empty",
            "Sec-Fetch-Mode": "cors",
            "Sec-Fetch-Site": "same-origin",    
            }
        
    def define_agent_and_referer(self, name_stock=None):
        '''assigns user-agent randomally and defines referrer based on type of request'''
        self.headers["User-Agent"] = random.choice(self.user_agent_list)
        if name_stock is None:
            self.headers["Referer"] = self.base_url 
        else:
            self.headers["Referer"] = self.base_url + "aktien/" + name_stock + "-aktie"

    def scrape_url(self, url, name_stock=None):
        '''returns beautiful soup from url if any. Else None'''
        self.define_agent_and_referer(name_stock)
        soup = None
        for x in range(0, self.n_retries):
            try:
                page = requests.get(url, headers=self.headers)# , cookies=cookies)
                if page.status_code == 200:
                    soup = BeautifulSoup(page.content, 'html.parser', from_encoding="utf-8")
                    break
            except Exception as e:
                print("error", str(e), " trying again for the ", x+1, " time")
            time.sleep(0.8 + random.random())
        return soup
    
    def get_links(self, isin_code, name):
        # get_urls_finanzen_xetra
        '''scrapes the finanzen page to get the url for termine and the name used by finanzen for the given isin
        returns: dictionairy with linls'''
        # find the website by symbol
        soup = self.scrape_url(self.search_url + isin_code)
        # for isin search (Frankfurt stocks): try to get directly the url from isin
        try:
            soup2 = soup.find("head")
            stock_url = soup2.find('link', href=lambda href: href and "www.finanzen.net/aktien/" in href)['href']
        except:
            print('error 4: no isin match')
            return {'isin': isin_code, 'name': name, 'symbol_finanzen':np.nan, 'name_finanzen':np.nan, 'stock_url':np.nan, 'termine_url':np.nan}
        # get the termine website from menu
        soup_3 = self.scrape_url(stock_url)
        try:
            termine_url = self.base_url + soup_3.find("a", "details-navigation__item-label", string="Termine", href=True)['href']
        except:
            print("error 3: no termine url")
            return {'isin': isin_code, 'name': name, 'symbol_finanzen':np.nan, 'name_finanzen':np.nan, 'stock_url':stock_url, 'termine_url':np.nan}
        name_finanzen = termine_url.replace("https://www.finanzen.net//termine/", "")    
        # get the symbol of finanzen
        try:
            symbol_finanzen = soup_3.find("em", "badge__key", string="Symbol").find_next_sibling("span").text 
        except:
            return {'isin': isin_code, 'name': name, 'symbol_finanzen':np.nan, 'name_finanzen':name_finanzen, 'stock_url':stock_url, 'termine_url':termine_url}
        return {'isin': isin_code, 'name': name, 'symbol_finanzen':symbol_finanzen, 'name_finanzen':name_finanzen, 'stock_url':stock_url, 'termine_url':termine_url}
    
    def scrape_termine(self, isin, termine_url, name_stock):
        # scrape_finanzen_termine
        '''scrapes the termine table of finanzen'''
        soup = self.scrape_url(termine_url, name_stock=name_stock)
        try:
            dates_table = soup.find_all("tbody")[2] #, "page-content__item page-content__item--space"
        except:
            print("error 1: no tabel")
            return [{'isin': isin, 'termine_url':termine_url, 'type':np.nan, 'info':np.nan, 'date':np.nan}]
        if "keine" in dates_table.text.lower().strip():
            print("error 2: no dates")
            return [{'isin': isin, 'termine_url':termine_url, 'type':np.nan, 'info':np.nan, 'date':np.nan}]
        else:
            count = 0
            dates = []
            # if there is table scrape the first 5 dates
            for tr in dates_table.find_all('tr')[0:5]:
                count += 1
                date = tr.find_all('td')
                dates = dates + [{'isin': isin, 'termine_url':termine_url, 'type':date[0].text.strip(), 'info':date[2].text.strip(), 'date':date[3].text.strip()}]
            return dates

    def get_all_dates(self, df):
        # scrape_dates(df):
        '''scrapes all the past dates from finanzen.net for a certain symbol and termine_url given in a dataframe'''
        dates = []
        for row in df.itertuples():
            if row.Index % 100 == 0:
                print(row.Index, row.isin, row.name_finanzen)
            dates += self.scrape_termine(row.isin, row.termine_url, row.name_finanzen)
            time.sleep(np.random.uniform(0.3, 0.8))
        df_dates = pd.DataFrame(dates)
        df_dates = pd.concat([df_dates.drop(columns=['date'].copy()), df_dates['date'].str.split(n=2, expand=True)], axis= 1)
        if 1 in df_dates.columns:
            df_dates.rename(columns={0:'date', 1:'estimate'}, inplace=True)
        else:
            df_dates.rename(columns={0:'date'}, inplace=True)
            df_dates['estimate'] = np.nan
        df_dates['date'] = pd.to_datetime(df_dates['date'], format="%d.%m.%Y")
        df_dates['estimate'] = np.where(df_dates['estimate'].isna(), 0, 1)
        print("code termine finished successfully")
        return df_dates
    
    def scrape_kgv_real(self, isin, kgv_old_url, rel_years, name_stock):
        # def scrape_finanzen_kgv_real(isin_code, kgv_old_url, rel_years):
        '''finds table of Unternehmenskennzahlen and scrapes th unverwässerte kgv for rel_years'''
        kgv_real = []
        soup_kgv = self.scrape_url(kgv_old_url, name_stock)
        try:
            table = soup_kgv.find("h2", string=lambda text: text and "unternehmenskennzahlen" in text.lower()).parent
            years = table.find_all("th")
            cur_kgv = table.find("label", "checkbox__label", string=lambda text: text and "kgv" in text.lower() and "unver" in text.lower()).parent.parent#, string=lambda text: text and "kgv" in text.lower())
        except:
            print("error 1: no Unternehmenskennzahlen Tabelle or KGV KPI")
            return [{'isin':isin, 'year':np.nan, 'kgv':np.nan}]
        for year in years:
            if year.text in rel_years:
                kgv_real.append({'isin':isin, 'year':year.text, 'kgv':cur_kgv.text})
            cur_kgv = cur_kgv.next_sibling
        return kgv_real

    def scrape_kgv_est(self, isin, kgv_est_url, rel_years, name_stock):
        '''finds table of Unternehmenskennzahlen and scrapes the estimate kgv for rel_years'''
        kgv_est = []
        soup_kgv = self.scrape_url(kgv_est_url, name_stock)
        try:
            table = soup_kgv.find("h1", string=lambda text: text and "schätzungen* zu" in text.lower()).parent
            years = table.find_all("th")
            cur_kgv = table.find("td", "table__td", string=lambda text: text and "kgv" in text.lower())
        except:
            print("error 1: no Unternehmenskennzahlen Tabelle or KGV KPI")
            return [{'isin':isin, 'year':rel_years[0], 'kgv':np.nan}]
            # get how many years to scrape (2023 and 2024)
        for year in years:
            if year.text in rel_years:
                kgv_est.append({'isin':isin, 'year':year.text, 'kgv':cur_kgv.text})
            cur_kgv = cur_kgv.next_sibling
        return kgv_est