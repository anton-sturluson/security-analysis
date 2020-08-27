from os import path
import json

import numpy as np
import pandas as pd

from utils import tools, COMPANYDIR

mapping = tools.get_mapping()
month2digit = mapping["month2digit"]
col2dtype = mapping["col2dtype"]

"""
TODO:
1. Sort x
    - dividend
    - history
    - stock split
2. Transpose financial statements x
    - income statement
    - balance sheet
    - cash flow
3. Remove redundant information from financial statements x
    - income statement
    - balance sheet
    - cash flow
4. Merge statistics with tmp.csv x
5. Rename columns
    - summary
    - statistics (with tmp)
6. Convert datatypes (string, datetime, bool, float)
    - income statement
    - balance sheet
    - cash flow
    - statistics
    - summary
7. Generate yearly data
"""
def sort_date_and_remove_nat(df):
    """Coerce datetime (index) and remove NaT."""
    df.index = pd.to_datetime(df.index, errors="coerce")
    df.index.name = "Date"
    # remove non-datetime rows (e.g. 'ttm')
    return df[~pd.isnull(df.index)].sort_index(ascending=False)

def transpose(df):
    """Transpose df, set 'Date' as index, and strip columns."""
    df = df.T.reset_index().rename(columns={"index" : "Date"})
    return df.set_index("Date")

def merge_statistics_df(statistics_df, symbol, debug):
    """Merge statistics.csv with tmp.csv."""
    tmp_df = tools.get_df("tmp", symbol, convert_index_to_datetime=False)
    if tmp_df is not None:
        tmp_df.index.name = "Date"
        if len(tmp_df) > 1: # only use the last row if multiple given
            tmp_df = pd.DataFrame(tmp_df.iloc[0, :]).T
        if len(statistics_df) > 0:
            tmp_df.index = [statistics_df.index[0]] # set index for merge
            # merge into one row
            new_row = statistics_df.merge(tmp_df, left_index=True, right_index=True)
            # concat with the rest of the dataframe (n-1 rows)
            new_df = pd.concat([new_row, statistics_df.iloc[1:, :]])

        else:
            tmp_df.index = [tools.get_today()]
            new_df = tmp_df

        if not debug:
            originalpath = path.join(COMPANYDIR, symbol, "original", "tmp_original.csv")
            tools.mv(path.join(COMPANYDIR, symbol,
                               tools.name_append(symbol, "tmp", filetype="csv")),
                     originalpath)
        return new_df

def rename_columns(columns):
    """Remove content inside parantheses and trailing digit from column names."""
    new_columns = []
    for col in columns:
        if "(" in col: # remove anything in parantheses
            col = col.split("(")[0]
        if col[-1].isdigit(): # remove trailing digit
            col = col[:len(col) - 1]
        if "Earnings Date" in col:
            col = "Earnings Date"
        new_columns.append(col.strip())
    return new_columns

def convert_dtypes(df):
    """Convert datatypes of columns in-place based on types defined in col2dtype."""
    def size2digit(text):
        if "M" in text:
            try:
                nfloat = len(text.split("M")[0].split(".")[1])
            except IndexError:
                nfloat = 0
            text = text.replace("M", "".join(["0"] * (6 - nfloat))).replace(".", "")
        if "B" in text:
            try:
                nfloat = len(text.split("B")[0].split(".")[1])
            except IndexError:
                nfloat = 0
            text = text.replace("B", "".join(["0"] * (9 - nfloat))).replace(".", "")
        if "T" in text:
            try:
                nfloat = len(text.split("T")[0].split(".")[1])
            except IndexError:
                nfloat = 0
            text = text.replace("T", "".join(["0"] * (12 - nfloat))).replace(".", "")
        if "k" in text:
            try:
                nfloat = len(text.split("k")[0].split(".")[1])
            except IndexError:
                nfloat = 0
            text = text.replace("k", "".join(["0"] * (3 - nfloat))).replace(".", "")
        return text

    def digitize_month(text):
        if not isinstance(text, str):
            return text

        for month in month2digit.keys():
            if month in text:
                text = text.replace(month, month2digit[month])
                break
        return text

    def percentage2float(text):
        try:
            return float(text.replace("%", "")) * 0.01
        except:
            return np.nan

    def to_numeric(text):
        if isinstance(text, float) and np.isnan(text):
            return text
        if "%" in text:
            return percentage2float(text)
        if "," in text:
            text = text.replace(",", "")
        text = size2digit(text)
        return pd.to_numeric(text, errors="coerce")

    for col in df:
        if col in col2dtype:
            if col2dtype[col] == "datetime":
                dates = df[col].apply(digitize_month)
                df[col] = dates.apply(pd.to_datetime, errors="coerce")

            elif col2dtype[col] == "bool":
                df[col] = df[col].astype(col2dtype[col])

            elif col2dtype[col] == "NotYetImplemented":
                pass

        elif df[col].dtype == "object":
            df[col] = df[col].apply(to_numeric)


def process_text(symbols, init, debug):
    for symbol in symbols:
        if not path.exists((originalpath := path.join(COMPANYDIR, symbol, "original"))):
            tools.mkdir(originalpath)

        # sort dividend.csv
        dividend_df = tools.get_df(filename := "dividend", symbol,
                                   convert_index_to_datetime=False)
        if dividend_df is not None:
            dividend_df = sort_date_and_remove_nat(dividend_df)
            tools.backup_and_save_df(filename, symbol, dividend_df, init, debug)

        # sort history.csv
        history_df = tools.get_df(filename := "history", symbol,
                                  convert_index_to_datetime=False)
        if history_df is not None:
            history_df = sort_date_and_remove_nat(history_df)
            tools.backup_and_save_df(filename, symbol, history_df, init, debug)

        # sort stock_split.csv
        stock_split_df = tools.get_df(filename := "stock_split", symbol,
                                      convert_index_to_datetime=False)
        if stock_split_df is not None:
            stock_split_df = sort_date_and_remove_nat(stock_split_df)
            tools.backup_and_save_df(filename, symbol, stock_split_df, init, debug)

        # transpose, sort and create yearly income_statement.csv
        income_statement_df = tools.get_df(filename := "income_statement", symbol,
                                           convert_index_to_datetime=False)
        if income_statement_df is not None and income_statement_df.index.name != "Date":
            income_statement_df = transpose(income_statement_df)
            income_statement_df = sort_date_and_remove_nat(income_statement_df)
            income_statement_df.columns = rename_columns(income_statement_df.columns)
            tools.backup_and_save_df(filename, symbol, income_statement_df, init, debug)

        # transpose, sort and create yearly balance_sheet.csv
        balance_sheet_df = tools.get_df(filename := "balance_sheet", symbol,
                                        convert_index_to_datetime=False)
        if balance_sheet_df is not None and balance_sheet_df.index.name != "Date":
            balance_sheet_df = transpose(balance_sheet_df)
            balance_sheet_df = sort_date_and_remove_nat(balance_sheet_df)
            balance_sheet_df.columns = rename_columns(balance_sheet_df.columns)
            tools.backup_and_save_df(filename, symbol, balance_sheet_df, init, debug)

        # transpose, sort and create yearly cash_flow.csv
        cash_flow_df = tools.get_df(filename := "cash_flow", symbol,
                                    convert_index_to_datetime=False)
        if cash_flow_df is not None and cash_flow_df.index.name != "Date":
            cash_flow_df = transpose(cash_flow_df)
            cash_flow_df = sort_date_and_remove_nat(cash_flow_df)
            cash_flow_df.columns = rename_columns(cash_flow_df.columns)
            tools.backup_and_save_df(filename, symbol, cash_flow_df, init, debug)

        # transpose, sort and merge statistics.csv
        statistics_df = tools.get_df(filename := "statistics", symbol,
                                     convert_index_to_datetime=False)
        if (statistics_df is not None
                and len(statistics_df.index)
                and statistics_df.index.name != "Date"):
            statistics_df = transpose(statistics_df)
            statistics_df = sort_date_and_remove_nat(statistics_df)
            statistics_df = merge_statistics_df(statistics_df, symbol, debug)
            if statistics_df is not None:
                statistics_df.columns = rename_columns(statistics_df.columns)
                tools.backup_and_save_df(filename, symbol, statistics_df, init, debug)

        #
        summary_df = tools.get_df(filename := "summary", symbol,
                                  convert_index_to_datetime=False)
        if summary_df is not None:
            summary_df.columns = rename_columns(summary_df.columns)
            tools.backup_and_save_df(filename, symbol, summary_df, init, debug)


def generate_mapping(symbols, debug):
    filenames = ["income_statement", "balance_sheet", "cash_flow"]
    # load mapping
    with open("crawler/mapping.json", "r") as r_obj:
        mapping = json.load(r_obj)
        col2filename = mapping["col2filename"]

    for symbol in symbols:
        for fname in filenames:
            if (df := tools.get_df(fname, symbol, debug=debug)) is not None:
                for col in df.columns:
                    if col not in col2filename:
                        print(col)
                        col2filename[col] = fname
    # update mapping
    with open("crawler/mapping.json", "w") as w_obj:
        json.dump(mapping, w_obj, indent=4)


def process(symbols, init, debug):
    process_text(symbols, init, debug)

    if init or debug:
        generate_mapping(symbols, debug)

    for symbol in symbols:
        for filename in \
                ["income_statement", "balance_sheet", "cash_flow",
                 "statistics", "summary"]:
            df = tools.get_df(filename, symbol, debug=debug, convert_index_to_datetime=True)
            if df is not None:
                convert_dtypes(df)
                tools.backup_and_save_df(filename, symbol, df, init, debug)
                print(f"processed {symbol}/{symbol}_{filename}.csv")


