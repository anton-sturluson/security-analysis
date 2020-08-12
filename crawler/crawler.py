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
from selenium.common.exceptions import TimeoutException, StaleElementReferenceException
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

from utils import tools, COMPANYDIR
from localpaths import DOWNLOADPATH, DRIVERPATH, ID, PASSWORD

# global variables
TIMEOUT = 5
MAX_TRIAL = 3

# URLs
def signinurl():
    return "https://login.yahoo.com/"
def summaryurl(t):
    return f"https://finance.yahoo.com/quote/{t}"
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
    def __init__(self, is_init, is_debug, is_test, n_symbol):
        self.is_init = is_init
        self.is_debug = is_debug
        self.is_test = is_test
        self.results = defaultdict(dict)
        if self.is_init:
            self.is_stocks = [None] * n_symbol
            self.currencys = [None] * n_symbol

        self.init_driver()
        self.signin()


    def init_driver(self):
        """Initialize ChromeDriver."""
        options = webdriver.ChromeOptions()
        if not (self.is_init or self.is_debug or self.is_test):
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
                 "profile.managed_default_content_settings.javascript" : 1,
                 "profile.managed_default_content_settings.plugins" : 1,
                 "profile.managed_default_content_settings.popups" : 2,
                 "profile.managed_default_content_settings.geolocation" : 2,
                 "profile.managed_default_content_settings.media_stream" : 2}
        #if not self.is_init: # cookie must be enabled to sign in
        #    prefs["profile.managed_default_content_settings.cookies"] = 2
        options.add_experimental_option("prefs", prefs)
        self.driver = webdriver.Chrome(DRIVERPATH, options=options)


    def signin(self):
        """Sign in Yahoo Finance using ID and password saved in localkeys.py."""
        # from https://stackoverflow.com/questions/48352380/org-openqa-selenium-invalidcookiedomainexception-document-is-cookie-averse-usin
        self.driver.get(signinurl())

        # send ID
        WebDriverWait(self.driver, TIMEOUT).until(
            EC.element_to_be_clickable((
                By.CSS_SELECTOR,
                "input[name='username']"))).send_keys(ID)
        # click submit
        WebDriverWait(self.driver, TIMEOUT).until(
            EC.element_to_be_clickable((
                By.CSS_SELECTOR,
                "input[name='signin']"))).click()
        # wait til password
        self.sleep()
        # send password
        WebDriverWait(self.driver, TIMEOUT).until(
            EC.element_to_be_clickable((
                By.CSS_SELECTOR,
                "input[name='password']"))).send_keys(PASSWORD)
        # click submit
        WebDriverWait(self.driver, TIMEOUT).until(
            EC.element_to_be_clickable((
                By.CSS_SELECTOR,
                "button[type='submit']"))).click()
        # wait til log-in
        self.sleep()


    def reboot(self):
        self.sleep(240, 300) # rest for [4, 5] minutes
        self.driver.quit()
        self.init_driver()
        self.signin()


    def close(self):
        """Close driver."""
        self.driver.close()


    def quit(self):
        """Quit driver."""
        self.driver.quit()


    def sleep(self, tmin=3, tmax=5):
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
        if "Date" in curdf:
            curdf.sort_index(ascending=False, inplace=True)
        curdf.to_csv(inpath, index=True)


    def get_today(self):
        today = datetime.today()
        premkt = today.replace(hour=8, minute=0, second=0, microsecond=0)
        if today < premkt:
            return (today - timedelta(days=1)).date()
        return today.date()


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


    def is_stock(self):
        """Return True if corresponding symbol is a stock else False."""
        # wait until sections are visible
        WebDriverWait(self.driver, TIMEOUT).until(
            EC.visibility_of_all_elements_located((
                By.CSS_SELECTOR,
                "div[id='quote-nav']>ul>li")))
        for section in self.driver.find_elements_by_css_selector(
                "div[id='quote-nav']>ul>li"):
            if "Financials" in section.text:
                return True

        return False


    def is_finished(self, result):
        """Return True if all elements in result is True else False."""
        return sum(result) == len(result)


    def exist(self, symbol):
        """Return True if symbol exists, else False."""
        res = False
        self.driver.get(summaryurl(symbol)) # check Summary section
        try:
            WebDriverWait(self.driver, TIMEOUT).until(
                EC.visibility_of_element_located((
                    By.CSS_SELECTOR,
                    "section[id='lookup-page']>section>div>h2")))

        except TimeoutException:
            res = True

        self.sleep()
        return res


    def mv_downloaded(self, symbol, from_, to_):
        """Move downloaded file from from_ to to_."""
        tools.mv(path.join(DOWNLOADPATH, from_),
                 tools.get_path(to_, symbol, is_debug=self.is_debug))


    def crawl_summary(self, symbol, i):
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

                    result[0] = True
                    self.sleep()

            if not result[1]:
                try:
                    self.driver.get(statisticsurl(symbol))
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
                    self.sleep()

            if self.is_finished(result):
                break
            self.sleep()

        name = "summary"
        if not self.is_finished(result):
            self.results[symbol][name] = result
        else:
            self.save(name, symbol, data)


    def crawl_history(self, symbol, i):
        """Crawl historical data.

            This includes:
                - Dividend history: symbol_dividend.csv
                - Stock price history: symbol_history.csv
                - Stock split history: symbol_stock_split.csv"""
        def download():
            WebDriverWait(self.driver, TIMEOUT).until( # click arrow
                EC.element_to_be_clickable((
                    By.CSS_SELECTOR,
                    "section>div>div>span>a"))).click()
            self.sleep() # wait to download

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
            self.sleep() # wait to load

        def switch_max():
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
            self.sleep()
            global is_max
            is_max = True

        # [Historical Prices, Dividends Only, Stock Splits]
        result = [False, False, False]
        is_max = False
        for _ in range(MAX_TRIAL):
            try:
                self.driver.get(historyurl(symbol))

                if self.is_init:
                    self.is_stocks[i] = self.is_stock()
                    self.sleep()
                    if not self.is_stocks[i]:
                        break

            except TimeoutException:
                self.sleep()
                continue

            downloaded = f"{symbol}.csv"
            if not result[0]:
                name = "history"
                if not self.is_debug and path.exists(tools.get_path(name,
                                                                    symbol)):
                    result[0] = True

                else:
                    try:
                        if not is_max:
                            switch_max()
                        # download
                        download()
                        # move summary.csv to data dir
                        self.mv_downloaded(symbol,
                                           downloaded,
                                           name)

                    except TimeoutException:
                        pass

                    else:
                        result[0] = True

            if not result[1]:
                name = "Dividends Only"
                if not self.is_debug and path.exists(tools.get_path(name,
                                                                    symbol)):
                    result[1] = True

                else:
                    try:
                        # switch to dividends
                        switch(name)
                        if not is_max:
                            switch_max()
                        # download
                        download()
                        # move divdend.csv to data dir
                        self.mv_downloaded(symbol, downloaded, name)

                    except TimeoutException:
                        pass

                    else:
                        result[1] = True

            if not result[2]:
                name = "Stock Splits"
                if not self.is_debug and path.exists(tools.get_path(name,
                                                                    symbol)):
                    result[2] = True
                else:
                    try:
                        # switch to dividends
                        switch(name)
                        if not is_max:
                            switch_max()
                        # click download
                        download()
                        # move split.csv to data dir
                        self.mv_downloaded(symbol, downloaded, name)

                    except TimeoutException:
                        pass

                    else:
                        result[2] = True

            if self.is_finished(result):
                break
            self.sleep()
            self.driver.refresh()
            is_max = False

        if not self.is_finished(result):
            if not self.is_init or (self.is_init and self.is_stocks[i]):
                self.results[symbol]["history"] = result


    def crawl_financials(self, symbol, i):
        """Crawl financial data.

            This includes:
                - Income Statement
                - Balance Sheet
                - Cash Flow"""
        def click_quarterly_and_download():
            """Click 'Quarterly' and 'Download'."""
            # click Quarterly
            WebDriverWait(self.driver, TIMEOUT).until(
                EC.element_to_be_clickable((
                    By.CSS_SELECTOR,
                    "section[data-test='qsp-financial']>div>div>button"))
                ).click()
            self.sleep() # wait to load

            # click Download
            WebDriverWait(self.driver, TIMEOUT).until(
                EC.element_to_be_clickable((
                    By.CSS_SELECTOR,
                    "section[data-test='qsp-financial'] div>span>button"))
                ).click()
            self.sleep() # wait to download

        def get_currency():
            tmp = WebDriverWait(self.driver, TIMEOUT).until(
                EC.visibility_of_element_located((
                    By.CSS_SELECTOR,
                    "section[data-test='qsp-financial']>div>span>span"))).text
            if "." in tmp:
                tmp = tmp.split(".")[0].split(" ")
                return tmp[2] if len(tmp) == 3 else None
            return "USD"

        # [Income Statement, Balance Sheet, Cash Flow]
        result = [False, False, False]
        for _ in range(MAX_TRIAL):

            if self.is_init and not self.is_stocks[i]:
                break

            if not result[0]:
                name = "income_statement"
                if not self.is_debug and path.exists(tools.get_path(name,
                                                                    symbol)):
                    result[0] = True

                else:
                    try:
                        self.driver.get(incomestatementurl(symbol))
                        click_quarterly_and_download()
                        self.mv_downloaded(symbol,
                                           f"{symbol}_quarterly_financials.csv",
                                           name)
                        if self.is_init:
                            if not self.is_stocks[i]:
                                break
                            self.currencys[i] = get_currency()

                    except TimeoutException:
                        pass

                    except StaleElementReferenceException:
                        self.reboot()

                    else:
                        result[0] = True

            if not result[1]:
                name = "balance_sheet"
                if not self.is_debug and path.exists(tools.get_path(name,
                                                                    symbol)):
                    result[1] = True

                else:
                    try:
                        self.driver.get(balancesheeturl(symbol))
                        click_quarterly_and_download()
                        self.mv_downloaded(symbol,
                                           f"{symbol}_quarterly_balance-sheet.csv",
                                           name)

                    except TimeoutException:
                        pass

                    except StaleElementReferenceException:
                        self.reboot()

                    else:
                        result[1] = True

            if not result[2]:
                name = "cash_flow"
                if not self.is_debug and path.exists(tools.get_path(name,
                                                                    symbol)):
                    result[2] = True

                else:
                    try:
                        self.driver.get(cashflowurl(symbol))
                        click_quarterly_and_download()
                        self.mv_downloaded(symbol,
                                           f"{symbol}_quarterly_cash-flow.csv",
                                           name)

                    except TimeoutException:
                        pass

                    except StaleElementReferenceException:
                        self.reboot()

                    else:
                        result[2] = True

            if self.is_finished(result):
                break
            self.sleep()

        if not self.is_finished(result):
            if not self.is_init or (self.is_init and self.is_stocks[i]):
                self.results[symbol]["financials"] = result


    def crawl_statistics(self, symbol, i):
        """Crawl statistics.csv."""
        result = [False, False]
        data = {}
        is_get = False
        for _ in range(MAX_TRIAL):
            name = "tmp"
            if not self.is_debug and path.exists(tools.get_path(name, symbol)):
                result[0] = True

            else:
                try:
                    self.driver.get(statisticsurl(symbol))
                    is_get = True
                    WebDriverWait(self.driver, TIMEOUT).until(
                        EC.visibility_of_element_located((
                            By.ID, "Main")))

                except TimeoutException:
                    pass

                else: # crawl statistics with bs4
                    html_content = self.driver.page_source
                    soup = BeautifulSoup(html_content, "html.parser")
                    for section in soup.find_all(
                            "section", {"data-test":"qsp-statistics"}):
                        for div in section.find_all("div"):
                            if ((h3 := div.find("h3")) is not None
                                and h3.text in {"Fiscal Year",
                                                "Profitability",
                                                "Management Effectiveness",
                                                "Income Statement",
                                                "Balance Sheet",
                                                "Cash Flow Statement"}):
                                for tr in div.find_all("tr"):
                                    col, val = self.parse(tr)
                                    data[col] = val
                    self.save(name, symbol, data)
                    self.sleep()
                    result[0] = True

            name = "statistics"
            if not self.is_debug and path.exists(tools.get_path(name, symbol)):
                result[1] = True

            else:
                try: # download quarterly statistics
                    if not is_get:
                        self.driver.get(statisticsurl(symbol))
                        self.sleep() # wait to load
                        is_get = True
                    WebDriverWait(self.driver, TIMEOUT).until(
                        EC.element_to_be_clickable((
                            By.CSS_SELECTOR,
                            "section[data-test='qsp-statistics'] div>span>button"))
                        ).click()
                    self.sleep()
                    # move downloaded file to symbol dir
                    self.mv_downloaded(symbol,
                                       f"{symbol}_quarterly_valuation_measures.csv",
                                       name)

                except TimeoutException:
                    pass

                except StaleElementReferenceException:
                    self.reboot()

                else:
                    result[1] = True

            if self.is_finished(result):
                break
            self.sleep()
            is_get = False

        if not self.is_finished(result):
            if not self.is_init or (self.is_init and self.is_stocks[i]):
                self.results[symbol]["statistics"] = result
