import time
import random
from os import path
from datetime import datetime, timedelta
from collections import defaultdict

import numpy as np
import pandas as pd
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.common.exceptions import TimeoutException
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

from utils import tools, COMPANYDIR
from localpaths import DOWNLOADPATH, DRIVERPATH

# global variables
TIMEOUT = 10
MAX_TRIAL = 3

# URLs
def summaryurl(t):
    return f"https://finance.yahoo.com/quote/{t}?p={t}&.tsrc=fin-srch"
def profileurl(t):
    return f"https://finance.yahoo.com/quote/{t}/profile?p={t}"
def statisticsurl(t):
    return f"https://finance.yahoo.com/quote/{t}/key-statistics?p={t}"
def incomestatementurl(t):
    return f"https://finance.yahoo.com/quote/{t}/financials?p={t}"
def balancesheeturl(t):
    return f"https://finance.yahoo.com/quote/{t}/balance-sheet?p={t}"
def cashflowurl(t):
    return f"https://finance.yahoo.com/quote/{t}/cash-flow?p={t}"
def historyurl(t):
    return f"https://finance.yahoo.com/quote/{t}/history?p={t}"


class ChromeDriver:
    def __init__(self, is_debug, is_init, n_symbol):
        self.is_debug = is_debug
        self.is_init = is_init
        self.results = defaultdict(dict)
        if self.is_init:
            self.is_stocks = [None] * n_symbol
            self.i = 0

        self.init_driver()

    def init_driver(self):
        """Initialize ChromeDriver."""
        options = webdriver.ChromeOptions()
        if not (self.is_init or self.is_debug):
            options.add_argument("--headless")
        options.add_argument("--incognito")
        options.add_argument("--disable-notifications")
        options.add_argument("--user-agent" \
                             "=''Mozilla/5.0 (Windows NT 10.0; Win64; x64)" \
                             " AppleWebKit/537.36 (KHTML, like Gecko)" \
                             " Chrome/74.0.3729.157 Safari/537.36''")
        # Disable unnecessary driver tips for speedup
        # From https://github.com/dinuduke/Selenium-chrome-firefox-tips
        prefs = {"profile.managed_default_content_settings.images" : 2,
                 "profile.default_content_setting_values.notifications" : 2,
                 "profile.managed_default_content_settings.stylesheets" : 2,
                 "profile.managed_default_content_settings.cookies" : 2,
                 "profile.managed_default_content_settings.javascript" : 1,
                 "profile.managed_default_content_settings.plugins" : 1,
                 "profile.managed_default_content_settings.popups" : 2,
                 "profile.managed_default_content_settings.geolocation" : 2,
                 "profile.managed_default_content_settings.media_stream" : 2}
        options.add_experimental_option("prefs", prefs)
        self.driver = webdriver.Chrome(DRIVERPATH, options=options)


    def close(self):
        """Close driver."""
        self.driver.close()


    def quit(self):
        """Quit driver."""
        self.driver.quit()


    def sleep(self, tmin=4, tmax=5):
        """Sleep crawler."""
        if tmin >= tmax:
            tmin = tmax - 2
        return time.sleep(random.uniform(tmin, tmax))


    def parse(self, tr):
        """Parse row element from table into column and value."""
        # choose any char not commonly used
        splitted = tr.get_text("|").split("|")
        return (" ".join(splitted[:len(splitted)-1]).strip(), # column
                splitted[-1]) # value


    def save(self, var, symbol, data, sep=",", index_col=0, is_backup=True):
        """Save data."""
        if not path.exists(dir_ := path.join(COMPANYDIR, symbol)):
            tools.mkdir(dir_)
        inpath = tools.get_path(var, symbol)

        if self.is_debug:
            if not path.exists(debugdir := path.join(dir_, "debug")):
                tools.mkdir(debugdir)
            inpath = tools.get_path(var, symbol, is_debug=self.is_debug)

        #  backup
        if not self.is_debug and is_backup and path.exists(inpath):
            if not path.exists(backdir := path.join(dir_, "backup")):
                tools.mkdir(backdir)
            tools.cp(inpath, tools.get_path(var, symbol, is_backup=True))

        # convert data to df
        if not isinstance(data, list):
            data = [data]
        curdf = pd.DataFrame(data)
        if "Date" in curdf:
            curdf["Date"] = (curdf["Date"].apply(
                lambda x: pd.to_datetime(x, errors="coerce"))
                             .astype("datetime64"))
            curdf.set_index("Date", inplace=True)

        if path.exists(inpath):
            # concat with existing file, remove any duplicate row
            maindf = tools.path2df(inpath, sep=sep, index_col=index_col)
            maindf = maindf[maindf.index != self.get_today().strftime("%Y-%m-%d")]
            curdf = pd.concat([curdf, maindf], axis=0)
        # sort and save
        curdf.sort_index(ascending=False).to_csv(inpath, index=True)


    def get_today(self):
        today = datetime.today()
        premkt = today.replace(hour=8, minute=0, second=0, microsecond=0)
        if today < premkt:
            return (today - timedelta(days=1)).date()
        return today.date()


    def is_stock(self, soup):
        """Return True if corresponding symbol to soup is stock else False.

            Note this information is stored if self.is_init==True and saved in profile."""
        if (div := soup.find("div", id="quote-nav")) is not None:
            sections = {sec for a in div.find_all("a")
                        if (sec := a.text) == "Financials"}
            return "Financials" in sections

        return False


    def preprocess(self, data):
        newdata = {} # make a new copy of preprocessed data
        for key, val in data.items():
            if key in ["Date", "Fiscal Year Ends"]:
                val = pd.to_datetime(val, errors="ignore")
                val = val.date() if isinstance(val, datetime) else val
                newdata[key] = np.datetime64("NaT") if val == "" else val

            else:
                # convert str (e.g. k, M, B, T) to number if exists
                val = tools.size2digit(val)
                # remove ',' if exists
                val = val.replace(",", "")
                # coerce to np.nan if not numeric
                newdata[key] = pd.to_numeric(val, errors="coerce")

        return newdata


    def is_finished(self, result):
        """Return True if all elements in result is True else False."""
        return sum(result) == len(result)


    def crawl_summary(self, symbol):
        """Crawl data to get saved in symbol_summary.csv."""
        data = {"Date" : self.get_today(), "Symbol" : symbol}
        # [Summary, Statistics]
        result = [False, False]
        for _ in range(MAX_TRIAL):
            if not result[0]: # crawl summary section
                try:
                    self.driver.get(summaryurl(symbol))

                    WebDriverWait(self.driver, TIMEOUT).until(
                        EC.visibility_of_all_elements_located((By.TAG_NAME,
                                                               "table")))

                except TimeoutException:
                    pass

                else:
                    html_content = self.driver.page_source
                    soup = BeautifulSoup(html_content, "html.parser")
                    for table in soup.find_all("table")[:2]:
                        for tr in table.find_all("tr"):
                            col, val = self.parse(tr)
                            data[col] = val

                    if self.is_init:
                        self.is_stocks[self.i] = self.is_stock(soup)

                    result[0] = True
                    if not self.is_finished(result):
                        self.sleep() # wait before reloading next page

            if not result[1]:
                self.driver.get(statisticsurl(symbol))
                try:
                    WebDriverWait(self.driver, TIMEOUT).until(
                        EC.visibility_of_element_located((
                            By.ID, "Main")))

                except TimeoutException:
                    pass

                else:
                    html_content = self.driver.page_source
                    soup = BeautifulSoup(html_content, "html.parser")
                    for section in soup.find_all(
                            "section", {"data-test":"qsp-statistics"}):
                        for div in section.find_all("div"):
                            if ((h3 := div.find("h3")) is not None
                                and h3.text in {"Stock Price History",
                                                "Share Statistics"}):
                                for tr in div.find_all("tr"):
                                    col, val = self.parse(tr)
                                    data[col] = val

                    result[1] = True

            if self.is_finished(result):
                break
            self.sleep()

        name = "summary"
        if not self.is_finished(result):
            self.results[symbol][name] = result
        self.save(name, symbol, data)

        if self.is_init: # update self.is_stocks index
            self.i += 1


    def crawl_history(self, symbol):
        """Crawl historical data.

            This includes:
                - symbol_dividend.csv
                - symbol_history.csv
                - symbol_stock_split.csv"""
        def download():
            WebDriverWait(self.driver, TIMEOUT).until( # click arrow
                EC.element_to_be_clickable((
                    By.CSS_SELECTOR,
                    "section>div>div>span>a"))).click()
            self.sleep(2, 3) # wait to download

        def switch(to_):
            WebDriverWait(self.driver, TIMEOUT).until( # click Show
                EC.element_to_be_clickable((
                    By.CSS_SELECTOR,
                    "section span>div[data-test='select-container']"))).click()
            menu = WebDriverWait(self.driver, TIMEOUT).until(
                EC.visibility_of_element_located((
                    By.CSS_SELECTOR,
                    "section span>div[data-test='historicalFilter-menu']")))
            for d in menu.find_elements_by_tag_name("div"):
                if d.text == to_:
                    d.click()
                    break
            WebDriverWait(self.driver, TIMEOUT).until( #click Apply
                EC.element_to_be_clickable((
                    By.CSS_SELECTOR,
                    "section>div>div>button"))).click()
            self.sleep(2, 3) # wait to load

        # [Historical Prices, Dividends Only, Stock Splits]
        result = [False, False, False]
        for _ in range(MAX_TRIAL):
            try:
                self.driver.get(historyurl(symbol))
                # click dropdown
                WebDriverWait(self.driver, TIMEOUT).until(
                    EC.element_to_be_clickable((
                        By.CSS_SELECTOR,
                        "section div[data-test='dropdown']>div"))).click()
                # click max
                WebDriverWait(self.driver, TIMEOUT).until(
                    EC.element_to_be_clickable((
                        By.CSS_SELECTOR, "li>button[data-value='MAX']"))
                    ).click()
                # wait to load
                self.sleep(2, 3)

            except TimeoutException:
                self.sleep()
                continue

            if not result[0]:
                try:
                    # download
                    download()
                    # move summary.csv to data dir
                    tools.mv(path.join(DOWNLOADPATH, f"{symbol}.csv"),
                             tools.get_path("history", symbol,
                                            is_debug=self.is_debug))

                except TimeoutException:
                    pass

                else:
                    result[0] = True

            if not result[1]:
                try:
                    what = "Dividends Only"
                    # switch to dividends
                    switch(what)
                    # download
                    download()
                    # move divdend.csv to data dir
                    tools.mv(path.join(DOWNLOADPATH, f"{symbol}.csv"),
                             tools.get_path(what, symbol, is_debug=self.is_debug))

                except TimeoutException:
                    pass

                else:
                    result[1] = True

            if not result[2]:
                try:
                    what = "Stock Splits"
                    # switch to dividends
                    switch(what)
                    # click download
                    download()
                    # move split.csv to data dir
                    tools.mv(path.join(DOWNLOADPATH, f"{symbol}.csv"),
                             tools.get_path(what, symbol, is_debug=self.is_debug))

                except TimeoutException:
                    pass

                else:
                    result[2] = True

            if self.is_finished(result):
                break
            self.sleep()

        name = "history"
        if not self.is_finished(result):
            self.results[symbol][name] = result
