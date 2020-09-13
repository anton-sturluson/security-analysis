import sys
import time
from datetime import datetime
from functools import partial
from os import cpu_count, path
from multiprocessing import Process

import schedule
import pandas as pd

from crawler.crawler import ChromeDriver
from crawler.preprocessor import init_process
from utils import tools, DATADIR, PROFILEPATH, PROFILEBACKPATH


def create_profile():
    """Merge .csv files in DATADIR and save to PROFILEPATH."""
    cols = ["Symbol", "Name", "IPOyear", "Sector", "Industry"]
    dfs = [(pd.read_csv(path.join(DATADIR, path_))[cols]
                .assign(Exchange=path_.split(".")[0].upper()))
           for path_ in ["amex.csv", "nasdaq.csv", "nyse.csv"]]
    df = pd.concat(dfs, axis=0)

    def process_symbol(symbol):
        symbol = symbol.replace("^", "-P")
        symbol = symbol.replace(".", "-")
        return symbol.strip()
    df["Symbol"] = df["Symbol"].apply(process_symbol)
    df = df.loc[df["Symbol"].apply(lambda x: "~" not in x), :]

    if path.exists(PROFILEPATH): # backup
        tools.mv(PROFILEPATH, PROFILEBACKPATH)
    df.sort_values("Symbol").to_csv(PROFILEPATH, index=False) # save

# switches
init = False
debug = False # run with first 5 symbols
schedule_crawler = True
override_profile = False
preprocess_only = False
force_summary = False
k = None
headless = True
for sarg in sys.argv[1:]:
    if sarg.startswith("--debug"):
        debug = True

    if sarg.startswith("--init"): # coerce init mode
        init = True

    if sarg.startswith("--no-schedule"):
        schedule_crawler = False

    if sarg.startswith("--override-profile"):
        override_profile = True

    if sarg.startswith("--preprocess-only"):
        preprocess_only = True
        schedule_crawler = False

    if sarg.startswith("--force-summary"):
        force_summary = True

    if sarg.startswith("--k="): # for testing real data
        k = int(sarg.split("=")[1])

    if sarg.startswith("--no-headless"):
        headless = False


def try_crawl(crawl_fn, symbol, fn_name):
    try:
        crawl_fn(symbol)
    except Exception as e:
        tools.log(f"[{symbol}] Failure crawling {fn_name}: {e}", debug)


def download_historical_data(symbols):
    if not preprocess_only:
        # Initialize driver
        driver = ChromeDriver(init, debug, headless)

        # Main loop for initial crawling
        for i, symbol in enumerate(symbols):
                if not driver.exist(symbol):
                    continue

                if driver.last_symbol_is_stock:
                    # crawl Historical Data section
                    try_crawl(driver.crawl_history, symbol, "history")
                    # crawl Financials section
                    try_crawl(driver.crawl_financials, symbol, "financials")
                    # crawl Statistics section
                    try_crawl(driver.crawl_statistics, symbol, "statistics")

                driver.reset_last_symbol_info()

        # quit driver
        driver.quit()

        tools.log("Failed results:", debug)
        tools.json_dump(driver.results, debug)

        if init:
            # store stock and currency information in stock_profile.csv
            profiledf = pd.read_csv(PROFILEPATH)
            profiledf["Stock"] = driver.stocks
            profiledf["Currency"] = driver.currencys
            profiledf.to_csv(PROFILEPATH, index=False)

    init_process(symbols, init, debug)


def crawl_summary_helper(symbols):
    driver = ChromeDriver(init, debug, headless)
    driver.crawl_summary(symbols)
    driver.quit()


def crawl_summary(symbols):
    if __name__ == "__main__":
        jobs = []
        for i in range(0, n := len(symbols), k := n // cpu_count()):
            jobs.append(Process(target=crawl_summary_helper,
                                args=(symbols[i : min(i + k, n)],)))
            jobs[-1].start()
        while jobs:
            jobs.pop().join()


if __name__ == "__main__":
    tools.log("=" * 42, debug)
    today = datetime.today()
    # Check https://docs.python.org/3/reference/lexical_analysis.html#f-strings for f-string
    tools.log(f"Starting crawler ({debug=}) - {today:%Y-%m-%d:%H-%M-%S}", debug)
    tools.log("=" * 42, debug)

    symbols = tools.get_symbols(stock_only=False if init else True)
    if k or debug:
        symbols = symbols[len(symbols)-k:len(symbols)]

    if not path.exists(PROFILEPATH) or override_profile:
        create_profile()
        init = True

    if init or preprocess_only:
        download_historical_data(symbols)
        init = False

    if force_summary:
        crawl_summary(symbols)

    if not debug and schedule_crawler:
        schedule.every().monday.at("19:00").do(crawl_summary, symbols)
        schedule.every().tuesday.at("19:00").do(crawl_summary, symbols)
        schedule.every().wednesday.at("19:00").do(crawl_summary, symbols)
        schedule.every().thursday.at("19:00").do(crawl_summary, symbols)
        schedule.every().friday.at("19:00").do(crawl_summary, symbols)

        i = 0
        while True:
            schedule.run_pending()
            time.sleep(1)
