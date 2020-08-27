# security-analysis

## Introduction

This repo consists of two main functions:
1. Web crawler
2. Bokeh dashboard

## How to run

1. Run `pip install -r requirements.txt` (also make sure your Python version is 3.8+).
2. Update `localpaths.py` with 1. correct paths to your download directory (where files get downloaded from Chrome Driver) and 2. to Chrome Driver. Also update Yahoo Finance 3. id and 4.password. You need subscription to download Financials section.
3. Run `python3 main.py` (or `python3 main.py --no-schedule` if you don't want to schedule automatic crawling).

## Acknowledgement

The following files are downloaded from [NASDAQ website](https://old.nasdaq.com/screening/company-list.aspx).
* data/amex.csv
* data/nasdaq.csv
* data/nyse.csv
