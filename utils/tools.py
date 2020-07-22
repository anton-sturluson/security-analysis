import csv
import sys
import json
from functools import lru_cache
from os import path, rename, system

import pandas as pd

from utils import PROFILEPATH, COMPANYDIR, LOGPATH, RESULTPATH
from crawler.mapping import var2filename, var2filetype


def cp(from_, to_):
    if path.exists(from_):
        system(f"cp {from_} {to_}")


def mv(from_, to_):
    if path.exists(from_):
        # check if path to the file exists
        tmp = "/".join((tmp := to_.split("/"))[0:len(tmp)-1])
        if not path.exists(tmp):
            mkdir(tmp)
        rename(from_, to_)


def mkdir(path):
    system(f"mkdir -p {path}")


def rm(path):
    if path.exists(path):
        system(f"rm {path}")


def log(msg, is_debug=False, is_print=True):
    if not is_debug:
        with open(LOGPATH, "a") as w_obj:
            w_obj.write(f"{msg.strip()}\n")

    if is_print:
        print(msg)


def json_dump(json_obj, is_debug=False, is_print=True, indent=4):
    if not is_debug:
        with open(RESULTPATH, "w") as w_obj:
            json.dump(json_obj, w_obj, indent=indent)

    if is_print:
        print(json.dumps(json_obj, indent=indent))


def get_symbols():
    return get_data("Symbol").values


def path2dir(path):
    return "/".join((tmp := path.split("/"))[:len(tmp)-1])


@lru_cache()
def get_path(var, symbol, is_yearly=False, is_backup=False, is_debug=False):
    if var2filename[var] == "profile":
        return PROFILEPATH

    if symbol is None:
        raise Exception("ERROR get_path(): symbol shouldn't be None if filename != 'profile'")

    t = "yearly" if is_yearly else "quarterly"
    file_ = (path_append(symbol, t, var2filename[var])
             if var2filename[var] in ["income_statement", "balance_sheet", "cash_flow"]
             else path_append(symbol, var2filename[var]))

    child = "backup" if is_backup else "debug"
    if is_backup or is_debug:
        return path.join(COMPANYDIR, symbol, child,
                         path_append(file_, child, filetype="csv"))
    else:
        return path.join(COMPANYDIR, symbol,
                         path_append(file_, filetype="csv"))


def path_append(*args, sep="_", filetype=None):
    tmp = sep.join(list(args))
    return (tmp if filetype is None
            else ".".join([tmp, filetype]))



@lru_cache()
def var2df(var, symbol=None, is_yearly=False, sep=",", index_col=0):
    if path.exists(inpath := get_path(var, symbol, is_yearly)):
        return path2df(inpath, sep=sep, index_col=index_col)


@lru_cache()
def path2df(path, sep=",", index_col=0):
    df = pd.read_csv(path, sep=sep, index_col=index_col)
    if index_col is not None:
        df = df[(~pd.isnull(df.index)) & (df.index != "ttm")]
        df.index = pd.to_datetime(df.index, errors="coerce")
    return df


@lru_cache()
def get_data(var,
             symbol=None,
             currency="USD",
             is_yearly=False,
             i_start=None,
             i_end=None,
             sep=","):
    def reader2next(r):
        try:
            return next(r)
        except StopIteration:
            return

    is_profile = True if symbol is None else False

    if not path.exists(inpath := get_path(var, symbol, is_yearly)):
        return

    if i_start is None and i_end is None: # return whole vector
        i_start = 0
        i_end = sys.maxsize # HMM...

    skip_ttm = (True if var2filename[var] in ["cash_flow", "income_statement"]
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

            if (tmp is None and ri > 0) or ri > 0:
                dates.append(prev["Date"]) if not is_profile else None
                if var2filetype[var] == "datetime":
                    res.append(pd.to_datetime(prev[var], errors="coerce"))
                elif var2filetype[var] == "float":
                    res.append(pd.to_numeric(prev[var], errors="coerce"))
                else: # string
                    res.append(prev[var])

            if tmp is None:
                break
            ri += 1

        if len(res) == 0:
            return

        if return_scala:
            return res[0]

        return pd.Series(res) if is_profile else pd.Series(res, dates)
