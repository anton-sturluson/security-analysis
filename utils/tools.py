import csv
import sys
import json
from datetime import datetime, timedelta
from functools import lru_cache
from os import path, rename, system

import numpy as np
import pandas as pd

from utils import PROFILEPATH, COMPANYDIR, LOGPATH, RESULTPATH, MAPPINGPATH


def get_mapping():
    with open(MAPPINGPATH, "r") as r_obj:
        return json.load(r_obj)

mapping = get_mapping()
col2filename = mapping["col2filename"]
col2dtype = mapping["col2dtype"]


def cp(from_, to_):
    if path.exists(from_):
        # check if destination dir exists
        tmp = "/".join((tmp := to_.split("/"))[0:len(tmp)-1])
        if not path.exists(tmp):
            mkdir(tmp)
        system(f"cp {from_} {to_}")


def mv(from_, to_):
    if path.exists(from_):
        # check if destination dir exists
        tmp = "/".join((tmp := to_.split("/"))[0:len(tmp)-1])
        if not path.exists(tmp):
            mkdir(tmp)
        rename(from_, to_)


def mkdir(path):
    system(f"mkdir -p {path}")


def rm(path):
    if path.exists(path):
        system(f"rm {path}")


def to_csv(df, outpath, index=True):
    n = len(splitted := outpath.split("/"))
    if not path.exists(outdir := path.join(*splitted[:n - 1])):
        mkdir(outdir)
    df.to_csv(outpath, index=index)



def log(msg, debug=False, verbose=True):
    if not debug:
        with open(LOGPATH, "a") as w_obj:
            w_obj.write(f"{msg.strip()}\n")

    if verbose:
        print(msg)


def json_dump(json_obj, debug=False, verbose=True, indent=4):
    if not debug:
        with open(RESULTPATH, "w") as w_obj:
            json.dump(json_obj, w_obj, indent=indent)
            w_obj.write("\n")

    if verbose:
        print(json.dumps(json_obj, indent=indent))


def get_symbols(stock_only=False):
    df = path2df(PROFILEPATH, index_col=None)
    if stock_only and "Stock" in df:
        mask = df["Stock"].fillna(False)
        return df["Symbol"][mask].values
    return df["Symbol"].values


def get_today():
    """Get the date at the point where data is crawled.

        If it is between 12am and 8am, return yeterday's date."""
    today = datetime.today()
    premkt = today.replace(hour=8, minute=0, second=0, microsecond=0)
    if today < premkt:
        return (today - timedelta(days=1)).date()
    return today.date()


def path2dir(path):
    return "/".join((tmp := path.split("/"))[:len(tmp)-1])


@lru_cache()
def get_path(col, symbol, yearly=False, backup=False, debug=False, original=False):
    if col2filename[col] == "profile":
        return PROFILEPATH

    if not symbol:
        raise Exception("ERROR get_path(): symbol shouldn't be None if filename != 'profile'")

    t = "yearly" if yearly else "quarterly"
    filename = (name_append(symbol, t, col2filename[col])
                if col2filename[col] in {"income_statement", "balance_sheet",
                                         "cash_flow", "statistics"}
                else name_append(symbol, col2filename[col]))

    if backup or debug or original:
        if backup:
            subpath = "backup"
        elif debug:
            subpath = "debug"
        elif original:
            subpath = "original"
        return path.join(COMPANYDIR, symbol, subpath,
                         name_append(filename, subpath, filetype="csv"))
    else:
        return path.join(COMPANYDIR, symbol,
                         name_append(filename, filetype="csv"))


@lru_cache()
def name_append(*args, sep="_", filetype=None):
    tmp = sep.join(list(args))
    return ".".join([tmp, filetype]) if filetype else tmp


@lru_cache()
def get_df(col, symbol=None, yearly=False, sep=",",
           index_col=0, convert_index_to_datetime=True, debug=False):
    if path.exists(inpath := get_path(col, symbol, yearly, debug=debug)):
        return path2df(inpath, sep, index_col, convert_index_to_datetime)



@lru_cache()
def path2df(path, sep=",", index_col=0, convert_index_to_datetime=True):
    df = pd.read_csv(path, sep=sep, index_col=index_col)
    if convert_index_to_datetime:
        df.index = pd.to_datetime(df.index, errors="coerce")
    return df


def backup_and_save_df(col, symbol, df, init, debug, index=True):
    outpath = get_path(col, symbol, debug=debug)
    if not debug and path.exists(outpath):
        if init:
            originalpath = get_path(col, symbol, original=True)
            cp(outpath, originalpath) # store the original file if init
        backpath = get_path(col, symbol, backup=True)
        mv(outpath, backpath)
    to_csv(df, outpath, index=index)


@lru_cache()
def get_data(col,
             symbol=None,
             yearly=False,
             i_start=None,
             i_end=None,
             sep=","):
    def reader2next(r):
        try:
            return next(r)
        except StopIteration:
            return

    profile = True if symbol is None else False

    if not path.exists(inpath := get_path(col, symbol, yearly)):
        return

    if i_start is None and i_end is None: # return whole vector
        i_start = 0
        i_end = sys.maxsize # HMM...

    skip_ttm = (True if col2filename[col] in ["cash_flow", "income_statement"]
                else False)

    with open(inpath, "r") as r_obj:
        reader = csv.DictReader(r_obj, delimiter=sep)

        if skip_ttm and reader2next(reader) is None:
            return

        return_scala = False
        if i_end is None:
            return_scala = True
            i_end = i_start

        ri = 0
        dates, res = [], []
        tmp = None
        while ri <= i_end:
            if tmp is not None:
                prev = tmp
            tmp = reader2next(reader)

            if ri > i_start:
                if not profile and prev["Symbol"] == symbol:
                    dates.append(prev["Date"])
                if col2dtype[col] == "datetime":
                    res.append(pd.to_datetime(prev[col], errors="coerce"))
                elif col2dtype[col] == "float":
                    res.append(pd.to_numeric(prev[col], errors="coerce"))
                elif col2dtype[col] == "bool":
                    res.append(bool(prev[col]))
                else: # string
                    res.append(prev[col])

                if return_scala:
                    return res[0]

            if tmp is None:
                break
            ri += 1

        if len(res) > 0:
            return pd.Series(res) if profile else pd.Series(res, dates)
