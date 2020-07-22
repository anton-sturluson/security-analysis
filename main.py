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
        return symbol
    df["Symbol"] = df["Symbol"].apply(process_symbol)
    df = df.loc[df["Symbol"].apply(lambda x: "~" not in x), :]
    df.sort_values("Symbol").to_csv(PROFILEPATH, index=False) # save

# switches
is_init = False
is_debug = False # run with first 5 symbols
is_test = False # run with k randomly selected symbols
is_schedule = True
for sarg in sys.argv[1:]:
    if sarg.startswith("--debug"):
        is_debug = True

    if sarg.startswith("--init"): # coerce init mode
        is_init = True

    if sarg.startswith("--test"):
        is_test = True

    if sarg.startswith("--no-schedule"):
        is_schedule = False


def main():
    # Initialize driver
    driver = crawler.ChromeDriver(is_debug, is_init,
                                  len(symbols := tools.get_symbols()))

    # Main loop
    if is_test:
        test_symbols = set(random.choices(symbols, k=100))
    for i, symbol in enumerate(symbols):
        if is_debug and i == 5:
            break

        if is_test and symbol not in test_symbols:
            continue

        if is_init:
            try:
                driver.crawl_history(symbol)
            except Exception as e:
                tools.log(f"[{symbol}] Failure crawling history: {e}", is_debug)

        try:
            driver.crawl_summary(symbol)
        except Exception as e:
            tools.log(f"[{symbol}] Failure crawling summary: {e}", is_debug)

    driver.quit()

    tools.log("Failed results:", is_debug)
    tools.json_dump(driver.results, is_debug)

    if is_init: # store boolean column IS_STOCK in stock_profile.csv
        profiledf = pd.read_csv(PROFILEPATH)
        profiledf["IS_STOCK"] = driver.is_stocks
        profiledf.to_csv(PROFILEPATH, index=False)


if __name__ == "__main__":
    tools.log("=" * 42, is_debug)
    today = datetime.today()
    # Check https://docs.python.org/3/reference/lexical_analysis.html#f-strings for f-string
    tools.log(f"Starting crawler ({is_debug=}) - {today:%Y-%m-%d:%H-%M-%S}", is_debug)
    tools.log("=" * 42, is_debug)

    if not os.path.exists(PROFILEPATH) or is_init:
        is_init = True
        create_profile()
        main()

    if not (is_debug or is_test) and is_schedule:
        schedule.every().monday.at("19:00").do(main)
        schedule.every().tuesday.at("19:00").do(main)
        schedule.every().wednesday.at("19:00").do(main)
        schedule.every().thursday.at("19:00").do(main)
        schedule.every().friday.at("19:00").do(main)

        i = 0
        while True:
            schedule.run_pending()
            time.sleep(60)
