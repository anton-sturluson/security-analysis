import time
from os import path
from collections import defaultdict

import pandas as pd
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, StaleElementReferenceException

from utils import tools, COMPANYDIR
from crawler.preprocessor import process_summary
from localpaths import DOWNLOADPATH, DRIVERPATH, ID, PASSWORD

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
    def __init__(self, init, debug, headless=True):
        self.init = init
        self.debug = debug
        self.results = defaultdict(dict)
        if self.init or self.debug:
            self.currency_of_last_symbol = None
            self.last_symbol_is_stock = None
            self.stocks = []
            self.currencys = []
        self.timeout = 5 # how many seconds to wait
        self.max_trial = 3 # how many times to try

        self.init_driver(headless and not self.init)
        if self.init:
            self.signin()


    def init_driver(self, headless):
        """Initialize ChromeDriver."""
        options = webdriver.ChromeOptions()
        if headless:
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
        if not self.init: # cookie must be enabled to sign in
            prefs["profile.managed_default_content_settings.cookies"] = 2
        options.add_experimental_option("prefs", prefs)
        self.driver = webdriver.Chrome(DRIVERPATH, options=options)


    def signin(self):
        """Sign in to Yahoo Finance using ID and password saved in localpaths.py."""
        self.driver.get(signinurl())
        # send username
        WebDriverWait(self.driver, self.timeout).until(
            EC.element_to_be_clickable((
                By.CSS_SELECTOR,
                "input[name='username']"))).send_keys(ID)
        # click 'Next'
        WebDriverWait(self.driver, self.timeout).until(
            EC.element_to_be_clickable((
                By.CSS_SELECTOR,
                "input[name='signin']"))).click()
        # wait til password
        self.sleep(3)
        # send password
        WebDriverWait(self.driver, self.timeout).until(
            EC.element_to_be_clickable((
                By.CSS_SELECTOR,
                "input[name='password']"))).send_keys(PASSWORD)
        # click submit
        WebDriverWait(self.driver, self.timeout).until(
            EC.element_to_be_clickable((
                By.CSS_SELECTOR,
                "button[type='submit']"))).click()
        # wait til log-in
        self.sleep(3)


    def reset_last_symbol_info(self):
        self.stocks.append(self.last_symbol_is_stock)
        self.currencys.append(self.currency_of_last_symbol)
        self.last_symbol_is_stock = None
        self.currency_of_last_symbol = None


    def reboot(self):
        """Reboot driver."""
        self.driver.quit()
        self.sleep(600) # rest for 10 minutes
        self.init_driver()
        self.signin()


    def close(self):
        """Close driver."""
        self.driver.close()


    def quit(self):
        """Quit driver."""
        self.driver.quit()


    def sleep(self, t=6):
        """Sleep crawler."""
        return time.sleep(t)


    def parse(self, tr):
        """Parse row element from table into column and value."""
        # choose any char not commonly used
        splitted = tr.get_text("|").split("|")
        val = (splitted[-1] if (col := splitted[0]) != "Earnings Date"
               else "".join(splitted[1:]))
        return col, val # column, value


    def save(self, col, symbol, data, sep=",", index_col=0, backup=True):
        """Save data."""
        if not path.exists(dir_ := path.join(COMPANYDIR, symbol)):
            tools.mkdir(dir_)
        inpath = tools.get_path(col, symbol)

        if self.debug:
            if not path.exists(debugdir := path.join(dir_, "debug")):
                tools.mkdir(debugdir)
            inpath = tools.get_path(col, symbol, debug=self.debug)

        #  backup
        if not self.debug and backup and path.exists(inpath):
            if not path.exists(backdir := path.join(dir_, "backup")):
                tools.mkdir(backdir)
            tools.cp(inpath, tools.get_path(col, symbol, backup=True))

        # convert data to df
        if not isinstance(data, list):
            data = [data]
        curdf = pd.DataFrame(data)
        curdf["Date"] = curdf["Date"].apply(tools.to_date)
        curdf.set_index("Date", inplace=True)
        process_summary(curdf)

        if path.exists(inpath):
            # concatenate with existing file, remove any duplicate row
            maindf = tools.path2df(inpath, sep=sep, index_col=index_col)
            maindf = maindf[maindf.index != tools.get_today()]
            curdf = pd.concat([curdf, maindf], axis=0)

        # sort and save
        curdf.sort_index(ascending=False, inplace=True)
        curdf.to_csv(inpath, index=True)


    def is_stock(self):
        """Return True if corresponding symbol is a stock else False."""
        try:
            # wait until sections are visible
            WebDriverWait(self.driver, self.timeout).until(
                EC.visibility_of_all_elements_located((
                    By.CSS_SELECTOR,
                    "div[id='quote-nav']>ul>li")))
            for section in self.driver.find_elements_by_css_selector(
                    "div[id='quote-nav']>ul>li"):
                if "Financials" in section.text:
                    return True

        except TimeoutException:
            return None

        else:
            return False


    def get_currency(self):
        tmp = WebDriverWait(self.driver, self.timeout).until(
            EC.visibility_of_element_located((
                By.CSS_SELECTOR,
                "section[data-test='qsp-financial']>div>span>span"))).text
        if "." in tmp:
            tmp = tmp.split(".")[0].split(" ") # split first sentence
            return tmp[2] if len(tmp) == 3 else None
        return "USD"


    def finished(self, result):
        """Return True if all elements in result is True else False."""
        return sum(result) == len(result)


    def exist(self, symbol):
        """Return True if symbol exists, else False."""
        self.driver.get(summaryurl(symbol)) # check Summary section
        try:
            WebDriverWait(self.driver, self.timeout).until(
                EC.visibility_of_element_located((
                    By.CSS_SELECTOR,
                    "section[id='lookup-page']>section>div>h2")))

        except TimeoutException:
            self.last_symbol_is_stock = self.is_stock()
            return True

        else:
            self.sleep()
            return False


    def mv_downloaded(self, symbol, from_, to_):
        """Move downloaded file from from_ to to_."""
        tools.mv(path.join(DOWNLOADPATH, from_),
                 tools.get_path(to_, symbol, debug=self.debug))


    def crawl_summary(self, symbols):
        """Crawl data to get saved in symbol_summary.csv."""
        for symbol in symbols:
            data = {"Date" : tools.get_today(), "Symbol" : symbol}
            # [Summary, Statistics]
            result = [False, False]
            for _ in range(self.max_trial):
                if not result[0]: # crawl summary section
                    try:
                        self.driver.get(summaryurl(symbol))

                        WebDriverWait(self.driver, self.timeout).until(
                            EC.visibility_of_all_elements_located((By.TAG_NAME,
                                                                   "table")))

                    except TimeoutException:
                        pass

                    except StaleElementReferenceException:
                        self.reboot()

                    else:
                        html_content = self.driver.page_source
                        soup = BeautifulSoup(html_content, "html.parser")
                        for table in soup.find_all("table")[:2]:
                            for tr in table.find_all("tr"):
                                col, val = self.parse(tr)
                                data[col] = val

                        result[0] = True
                        self.sleep(3)

                if not result[1]:
                    try:
                        self.driver.get(statisticsurl(symbol))
                        WebDriverWait(self.driver, self.timeout).until(
                            EC.visibility_of_element_located((
                                By.ID, "Main")))

                    except TimeoutException:
                        pass

                    except StaleElementReferenceException:
                        self.reboot()

                    else:
                        html_content = self.driver.page_source
                        soup = BeautifulSoup(html_content, "html.parser")
                        for section in soup.find_all(
                                "section", {"data-test":"qsp-statistics"}):
                            for div in section.find_all("div"):
                                children = list(div.children)
                                if len(children) == 2 and children[0].text in {
                                        "Stock Price History", "Share Statistics"}:
                                    for tr in children[1].find_all("tr"):
                                        col, val = self.parse(tr)
                                        data[col] = val

                        result[1] = True
                        self.sleep(3)

                if self.finished(result):
                    break

            name = "summary"
            if not self.finished(result):
                self.results[symbol][name] = result
            else:
                self.save(name, symbol, data)


    def crawl_history(self, symbol):
        """Crawl historical data.

            This includes:
                - Dividend history: symbol_dividend.csv
                - Stock price history: symbol_history.csv
                - Stock split history: symbol_stock_split.csv"""
        def download():
            WebDriverWait(self.driver, self.timeout).until( # click arrow
                EC.element_to_be_clickable((
                    By.CSS_SELECTOR,
                    "section>div>div>span>a"))).click()
            self.sleep(3) # wait to download

        def switch(to_):
            WebDriverWait(self.driver, self.timeout).until( # click Show
                EC.element_to_be_clickable((
                    By.CSS_SELECTOR,
                    "section span>div[data-test='select-container']"))).click()
            menu = WebDriverWait(self.driver, self.timeout).until(
                EC.visibility_of_element_located((
                    By.CSS_SELECTOR,
                    "section span>div[data-test='historicalFilter-menu']")))
            for d in menu.find_elements_by_tag_name("div"):
                if d.text == to_:
                    d.click()
                    break
            WebDriverWait(self.driver, self.timeout).until( #click Apply
                EC.element_to_be_clickable((
                    By.CSS_SELECTOR,
                    "section>div>div>button"))).click()
            self.sleep(3) # wait to load

        def switch_max():
            # click dropdown
            WebDriverWait(self.driver, self.timeout).until(
                EC.element_to_be_clickable((
                    By.CSS_SELECTOR,
                    "section div[data-test='dropdown']>div"))).click()

            # click max
            WebDriverWait(self.driver, self.timeout).until(
                EC.element_to_be_clickable((
                    By.CSS_SELECTOR, "li>button[data-value='MAX']"))
                ).click()

            # wait to load
            self.sleep(3)
            global is_max
            is_max = True

        if self.last_symbol_is_stock:
            self.driver.get(historyurl(symbol))
            is_max = False
            # [Historical Prices, Dividends Only, Stock Splits]
            result = [False, False, False]
            for _ in range(self.max_trial):
                downloaded = f"{symbol}.csv"
                if not result[0]:
                    name = "history"
                    if not self.debug and path.exists(tools.get_path(name,
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

                        except StaleElementReferenceException:
                            self.reboot()

                        else:
                            result[0] = True

                if not result[1]:
                    name = "Dividends Only"
                    if not self.debug and path.exists(tools.get_path(name,
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

                        except StaleElementReferenceException:
                            self.reboot()

                        else:
                            result[1] = True

                if not result[2]:
                    name = "Stock Splits"
                    if not self.debug and path.exists(tools.get_path(name,
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

                        except StaleElementReferenceException:
                            self.reboot()

                        else:
                            result[2] = True

                if self.finished(result):
                    break
                self.driver.refresh()

            if not self.finished(result):
                self.results[symbol]["history"] = result
            self.sleep()


    def crawl_financials(self, symbol):
        """Crawl financial data.

            This includes:
                - Income Statement
                - Balance Sheet
                - Cash Flow"""
        def click_quarterly_and_download():
            """Click 'Quarterly' and 'Download'."""
            # click Quarterly
            WebDriverWait(self.driver, self.timeout).until(
                EC.element_to_be_clickable((
                    By.CSS_SELECTOR,
                    "section[data-test='qsp-financial']>div>div>button"))
                ).click()
            self.sleep(3) # wait to load

            # click Download
            WebDriverWait(self.driver, self.timeout).until(
                EC.element_to_be_clickable((
                    By.CSS_SELECTOR,
                    "section[data-test='qsp-financial'] div>span>button"))
                ).click()
            self.sleep(3) # wait to download

        if self.last_symbol_is_stock:
            # [Income Statement, Balance Sheet, Cash Flow]
            result = [False, False, False]
            for _ in range(self.max_trial):

                if not result[0]:
                    name = "income_statement"
                    if self.init or self.debug:
                        try:
                            self.driver.get(incomestatementurl(symbol))
                            self.currency_of_last_symbol = self.get_currency()

                            if not path.exists(tools.get_path(name, symbol)):
                                click_quarterly_and_download()
                                self.mv_downloaded(symbol,
                                                   f"{symbol}_quarterly_financials.csv",
                                                   name)

                        except TimeoutException:
                            pass

                        except StaleElementReferenceException:
                            self.reboot()

                        else:
                            result[0] = True

                if not result[1]:
                    name = "balance_sheet"
                    if not self.debug and path.exists(tools.get_path(name,
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
                    if not self.debug and path.exists(tools.get_path(name,
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

                if self.finished(result):
                    break

            if not self.finished(result):
                self.results[symbol]["financials"] = result

            self.sleep()


    def crawl_statistics(self, symbol):
        """Crawl statistics.csv."""
        result = [False, False]
        data = {}
        self.driver.get(statisticsurl(symbol))
        self.sleep(3)
        for _ in range(self.max_trial):
            name = "tmp"
            if not self.debug and path.exists(tools.get_path(name, symbol)):
                result[0] = True

            else:
                try:
                    WebDriverWait(self.driver, self.timeout).until(
                        EC.visibility_of_element_located((
                            By.ID, "Main")))

                except TimeoutException:
                    pass

                except StaleElementReferenceException:
                    self.reboot()

                else: # crawl statistics with bs4
                    html_content = self.driver.page_source
                    soup = BeautifulSoup(html_content, "html.parser")
                    for section in soup.find_all(
                            "section", {"data-test":"qsp-statistics"}):
                        for div in section.find_all("div"):
                            children = list(div.children)
                            if len(children) == 2 and children[0].text in {
                                    "Fiscal Year", "Profitability",
                                    "Management Effectiveness",
                                    "Income Statement", "Balance Sheet",
                                    "Cash Flow Statement", "Dividends & Splits"}:
                                for tr in children[1].find_all("tr"):
                                    col, val = self.parse(tr)
                                    data[col] = val

                    self.save(name, symbol, data)
                    result[0] = True

            name = "statistics"
            if not self.debug and path.exists(tools.get_path(name, symbol)):
                result[1] = True

            else:
                try: # download quarterly statistics
                    WebDriverWait(self.driver, self.timeout).until(
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

            if self.finished(result):
                break

        if not self.finished(result):
            self.results[symbol]["statistics"] = result

        self.sleep()


    def crawl_profile_info(self, symbols):
        """Crawl 'Stock' and 'Currency' columns in stock_profile.csv."""
        data = {"Stock" : [False for _ in range(len(symbols))],
                "Currency" : [None for _ in range(len(symbols))]}
        for i, symbol in enumerate(symbols):
            if self.exist(symbol):
                try:
                    # crawl 'Stock' column
                    is_stock = self.is_stock()
                    self.sleep(3)
                    # crawl 'Currency' column
                    self.driver.get(incomestatementurl(symbol))
                    currency = self.get_currency()
                    self.sleep(3)

                except:
                    pass

                else:
                    data["Stock"][i] = is_stock
                    data["Currency"][i] = currency

        return data

