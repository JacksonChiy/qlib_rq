import pandas as pd
import re
import copy
import importlib
import time
import bisect
import pickle
import random
import requests
import functools
from pathlib import Path
from typing import Iterable, Tuple, List

import numpy as np
import pandas as pd
from loguru import logger
from yahooquery import Ticker
from tqdm import tqdm
from functools import partial
from concurrent.futures import ProcessPoolExecutor
from bs4 import BeautifulSoup
from data_collector.utils import (
    get_1d_data,
    generate_minutes_calendar_from_daily,
    calc_paused_num

)

def calc_adjusted_price(
    df: pd.DataFrame,
    _1d_data_all: pd.DataFrame,
    _date_field_name: str,
    _symbol_field_name: str,
    frequence: str,
    consistent_1d: bool = True,
    calc_paused: bool = True,
    am_range: Tuple[str, str] = ("09:30:00", "11:29:00"),
    pm_range: Tuple[str, str] = ("13:00:00", "14:59:00"),
) -> pd.DataFrame:
    """calc adjusted price
    This method does 4 things.
    1. Adds the `paused` field.
        - The added paused field comes from the paused field of the 1d data.
    2. Aligns the time of the 1d data.
    3. The data is reweighted.
        - The reweighting method:
            - volume / factor
            - open * factor
            - high * factor
            - low * factor
            - close * factor
    4. Called `calc_paused_num` method to add the `paused_num` field.
        - The `paused_num` is the number of consecutive days of trading suspension.
    """
    # TODO: using daily data factor
    if df.empty:
        return df
    df = df.copy()
    df.drop_duplicates(subset=_date_field_name, inplace=True)
    df.sort_values(_date_field_name, inplace=True)
    symbol = df.iloc[0][_symbol_field_name]
    df[_date_field_name] = pd.to_datetime(df[_date_field_name])
    # get 1d data from qlib
    _start = pd.Timestamp(df[_date_field_name].min()).strftime("%Y-%m-%d")
    _end = (pd.Timestamp(df[_date_field_name].max()) + pd.Timedelta(days=1)).strftime("%Y-%m-%d")
    data_1d: pd.DataFrame = get_1d_data(_date_field_name, _symbol_field_name, symbol, _start, _end, _1d_data_all)
    data_1d = data_1d.copy()
    if data_1d is None or data_1d.empty:
        df["factor"] = 1 / df.loc[df["close"].first_valid_index()]["close"]
        # TODO: np.nan or 1 or 0
        df["paused"] = np.nan
    else:
        # NOTE: volume is np.nan or volume <= 0, paused = 1
        # FIXME: find a more accurate data source
        data_1d["paused"] = 0
        data_1d.loc[(data_1d["volume"].isna()) | (data_1d["volume"] <= 0), "paused"] = 1
        data_1d = data_1d.set_index(_date_field_name)

        # add factor from 1d data
        # NOTE: 1d data info:
        #   - Close price adjusted for splits. Adjusted close price adjusted for both dividends and splits.
        #   - data_1d.adjclose: Adjusted close price adjusted for both dividends and splits.
        #   - data_1d.close: `data_1d.adjclose / (close for the first trading day that is not np.nan)`
        def _calc_factor(df_1d: pd.DataFrame):
            try:
                _date = pd.Timestamp(pd.Timestamp(df_1d[_date_field_name].iloc[0]).date())
                df_1d["factor"] = data_1d.loc[_date]["close"] / df_1d.loc[df_1d["close"].last_valid_index()]["close"]
                df_1d["paused"] = data_1d.loc[_date]["paused"]
            except Exception:
                df_1d["factor"] = np.nan
                df_1d["paused"] = np.nan
            return df_1d

        df = df.groupby([df[_date_field_name].dt.date], group_keys=False).apply(_calc_factor)
        if consistent_1d:
            # the date sequence is consistent with 1d
            df.set_index(_date_field_name, inplace=True)
            df = df.reindex(
                generate_minutes_calendar_from_daily(
                    calendars=pd.to_datetime(data_1d.reset_index()[_date_field_name].drop_duplicates()),
                    freq=frequence,
                    am_range=am_range,
                    pm_range=pm_range,
                )
            )
            df[_symbol_field_name] = df.loc[df[_symbol_field_name].first_valid_index()][_symbol_field_name]
            df.index.names = [_date_field_name]
            df.reset_index(inplace=True)
    for _col in ["open", "close", "high", "low", "volume"]:
        if _col not in df.columns:
            continue
        if _col == "volume":
            df[_col] = df[_col] / df["factor"]
        else:
            df[_col] = df[_col] * df["factor"]
    if calc_paused:
        df = calc_paused_num(df, _date_field_name, _symbol_field_name)
    return df