import os
import sys
import time
import random
from datetime import datetime

import schedule
import pandas as pd

from crawler import crawler
from utils import tools, DATADIR, PROFILEPATH


def create_profile():
    """Merge .csv files in DATADIR and save to PROFILEPATH."""
    cols = ["Symbol", "Name", "IPOyear", "Sector", "Industry"]
    dfs = [(pd.read_csv(os.path.join(DATADIR, path_))[cols]
                .assign(Exchange=path_.split(".")[0].upper()))
           for path_ in ["amex.csv", "nasdaq.csv", "nyse.csv"]]
    df = pd.concat(dfs, axis=0)

    def process_symbol(symbol):
        symbol = symbol.replace("^", "-P")
        symbol = symbol.replace(".", "-")
        return symbol.strip()
    df["Symbol"] = df["Symbol"].apply(process_symbol)
    df = df.loc[df["Symbol"].apply(lambda x: "~" not in x), :]
    df.sort_values("Symbol").to_csv(PROFILEPATH, index=False) # save

# switches
is_init = False
is_debug = False # run with first 5 symbols
is_test = False # run with k randomly selected symbols
is_schedule = True
is_override = False
for sarg in sys.argv[1:]:
    if sarg.startswith("--debug"):
        is_debug = True

    if sarg.startswith("--init"): # coerce init mode
        is_init = True

    if sarg.startswith("--test"):
        is_test = True

    if sarg.startswith("--no-schedule"):
        is_schedule = False

    if sarg.startswith("--override-profile"):
        is_override = True

def try_crawl(crawl_fn, symbol, i, fn_name):
    try:
        crawl_fn(symbol, i)
    except Exception as e:
        tools.log(f"[{symbol}] Failure crawling {fn_name}: {e}", is_debug)

def main():
    # Initialize driver
    symbols = tools.get_symbols(stock_only=False if is_init else True)
    driver = crawler.ChromeDriver(is_init, is_debug, is_test, len(symbols))

    # Main loop
    if is_test:
        test_symbols = set(random.choices(symbols, k=100))
    for i, symbol in enumerate(symbols):
        if is_debug and i == 5:
            break

        if is_test and symbol not in test_symbols:
            continue

        if is_init or is_debug or is_test:
            if not driver.exist(symbol):
                continue

            # crawl Historical Data section
            try_crawl(driver.crawl_history, symbol, i, "history")
            # crawl Financials section
            try_crawl(driver.crawl_financials, symbol, i, "financials")
            # crawl Statistics section
            try_crawl(driver.crawl_statistics, symbol, i, "statistics")

        elif is_schedule or is_debug or is_test:
            # crawl daily Summary + Statistics section
            try_crawl(driver.crawl_summary, symbol, i, "summary")

    driver.quit()

    tools.log("Failed results:", is_debug)
    tools.json_dump(driver.results, is_debug)

    # store boolean column IS_STOCK in stock_profile.csv
    profiledf = pd.read_csv(PROFILEPATH)
    if is_init and is_override:
        profiledf["IS_STOCK"] = driver.is_stocks
        profiledf["Currency"] = driver.currencys
        profiledf.to_csv(PROFILEPATH, index=False)


if __name__ == "__main__":
    tools.log("=" * 42, is_debug)
    today = datetime.today()
    # Check https://docs.python.org/3/reference/lexical_analysis.html#f-strings for f-string
    tools.log(f"Starting crawler ({is_debug=}) - {today:%Y-%m-%d:%H-%M-%S}", is_debug)
    tools.log("=" * 42, is_debug)

    if not os.path.exists(PROFILEPATH) or is_override:
        create_profile()
        is_init = True

    if is_init or is_debug or is_test:
        main()
        is_init = False

    if not is_debug and is_schedule:
        schedule.every().monday.at("19:00").do(main)
        schedule.every().tuesday.at("19:00").do(main)
        schedule.every().wednesday.at("19:00").do(main)
        schedule.every().thursday.at("19:00").do(main)
        schedule.every().friday.at("19:00").do(main)

        i = 0
        while True:
            schedule.run_pending()
            time.sleep(1)
