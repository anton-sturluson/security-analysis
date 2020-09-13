# WRITE TEST
import os
from datetime import datetime
from multiprocessing import Process
from crawler.preprocessor import init_process
from crawler.crawler import ChromeDriver
from utils import tools, DATADIR, PROFILEPATH, PROFILEBACKPATH

def test_sequential_crawl_summary():
    init, debug, headless = False, True, True
    driver = ChromeDriver(init, debug, headless)
    symbols = ["AAPL", "TDOC", "NKLA"]

    driver.crawl_summary(symbols)
    driver.crawl_summary(symbols)

    for symbol in symbols:
        df = tools.get_df("summary", symbol, debug=True)
        columns = set(df.columns)
        assert not df.index.duplicated().sum(), f"{symbol} duplicated dates"
        assert df.index[0] == tools.get_today(), f"{Symbol} has wrong date in the first row"
        for col in ["Ask", "EPS", "Short Ratio"]:
            assert(col in columns)

# test sequential crawling
# test_sequential_crawl_summary()

def crawl_summary_helper(symbols, init, debug, headless):
    driver = ChromeDriver(init, debug, headless)
    driver.crawl_summary(symbols)
    driver.quit()

def test_parallel_crawl_summary():
    if __name__ == "__main__":
        init, debug, headless = False, True, True
        n_cpu = os.cpu_count()
        symbols = tools.get_symbols(True)[:2 * n_cpu]

        jobs = []
        for i in range(0, n := len(symbols), k := n // n_cpu):
            p = Process(target=crawl_summary_helper,
                        args=(symbols[i : min(i + k, n)],
                              init, debug, headless))
            jobs.append(p)
            p.start()

        while jobs:
            p = jobs.pop()
            p.join()

        for symbol in symbols:
            df = tools.get_df("summary", symbol, debug=debug)
            columns = set(df.columns)
            assert not df.index.duplicated().sum(), f"{symbol} duplicated dates"
            assert df.index[0] == tools.get_today(), f"ERROR: {symbol} failed to crawl"

            for col in ["Ask", "EPS", "Short Ratio"]:
                assert col in columns, f"ERROR: {symbol} has no {col}"

# test parallel crawling
test_parallel_crawl_summary()
