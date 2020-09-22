import sys
import time
import argparse
import traceback
from datetime import datetime
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


def try_crawl(crawl_fn, symbol, fn_name):
    try:
        crawl_fn(symbol)
    except Exception as e:
        tools.log(f"[{symbol}] Failure crawling {fn_name}: {e}", debug)
        traceback.print_exc()


def download_historical_data(symbols):
    if not process_only:
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

    init_process(symbols, init, debug)


def crawl_summary_helper(symbols):
    driver = ChromeDriver(init, debug, headless)
    driver.crawl_summary(symbols)
    driver.quit()


def crawl_summary(symbols):
    if __name__ == "__main__":
        jobs = []
        k = cpu_count()
        for i in range(k):
            jobs.append(Process(target=crawl_summary_helper,
                                args=(symbols[i:len(symbols):k],)))
            jobs[-1].start()
            time.sleep(10)
        while jobs:
            jobs.pop().join()


def crawl_profile_info_helper(symbols):
    driver = ChromeDriver(init, debug, headless)
    res = driver.crawl_profile_info(symbols)
    driver.quit()
    return res


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--init", action="store_true", dest="init",
                       help="Run initial crawling")
    parser.add_argument("--debug", action="store_true", dest="debug",
                       help="Run crawler in debug mode")
    parser.add_argument("--no-schedule", action="store_true", dest="no_schedule",
                       help="Schedule automatic crawling")
    parser.add_argument("--override-profile", action="store_true", dest="override_profile",
                       help="Override stock_profile.csv")
    parser.add_argument("--process-only", action="store_true", dest="process_only",
                       help="Skip crawling")
    parser.add_argument("--force-summary", action="store_true", dest="force_summary",
                       help="Run crawler to get summary.csv")
    parser.add_argument("--k", action="store", default=None, dest="k",
                       help="Select number of symbols to crawl")
    parser.add_argument("--no-headless", action="store_true", dest="no_headless",
                       help="Run crawler in headless mode")
    parser.add_argument("--profile-info", action="store_true", dest="profile_info",
                       help="Crawl profile info")
    return parser.parse_args()

parser = parse_args()
# switches
init = parser.init
debug = parser.debug # run with first 5 symbols
schedule_crawler = not parser.no_schedule
override_profile = parser.override_profile
process_only = parser.process_only
force_summary = parser.force_summary
k = parser.k
headless = not parser.no_headless
profile_info = parser.profile_info

if __name__ == "__main__":
    tools.log("=" * 42, debug)
    today = datetime.today()
    # Check https://docs.python.org/3/reference/lexical_analysis.html#f-strings for f-string
    tools.log(f"Starting crawler ({debug=}) - {today:%Y-%m-%d:%H-%M-%S}", debug)
    tools.log("=" * 42, debug)

    symbols = tools.get_symbols(stock_only=False if override_profile else True)
    if k or debug:
        k = k if k else 5
        symbols = symbols[len(symbols)-k:len(symbols)]

    if not path.exists(PROFILEPATH) or override_profile:
        create_profile()

    if profile_info:
        res = crawl_profile_info_helper(symbols)
        pd.DataFrame(res).to_csv("tmp.csv")
        if override_profile:
            # store stock and currency information in stock_profile.csv
            profiledf = tools.path2df(PROFILEPATH)
            profiledf["Stock"] = res["Stock"]
            profiledf["Currency"] = res["Currency"]
            tools.to_csv(profiledf, PROFILEPATH, index=False)

    if init or process_only:
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
