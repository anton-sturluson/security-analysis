# security-analysis-tutorial

## Introduction

This is a source code for [Security Analysis with Python](https://medium.com/semper-augustus) in Medium.

Currently, the following contents are implemented:
1. [Web Crawler Part I: How to collect financial data from Yahoo Finance](https://medium.com/semper-augustus/security-analysis-with-python-web-crawler-part-i-how-to-collect-financial-data-from-yahoo-5c326924052c) in branch [tutorial\_0.1](https://github.com/anton-sturluson/security-analysis-tutorial/tree/tutorial_0.1).

## How to run

1. Run `pip install -r requirements.txt` (also make sure your Python version is 3.8+).
2. Update `localpaths.py` with correct paths to your download directory (where files get downloaded from Chrome Driver) and to Chrome Driver.
3. Run `python3 main.py` (or `python3 main.py --no-schedule` if you don't want to schedule automatic crawling).

## Acknowledgement

The following files are downloaded from [NASDAQ website](https://old.nasdaq.com/screening/company-list.aspx).
* data/amex.csv
* data/nasdaq.csv
* data/nyse.csv
