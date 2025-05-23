# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

import abc
import sys
import copy
import time
import datetime
import importlib
from abc import ABC
import multiprocessing
from pathlib import Path
from typing import Iterable

import fire
import numpy as np
import pandas as pd
from loguru import logger
import rqdatac as rq

import qlib
from qlib.data import D
from qlib.tests.data import GetData
from qlib.utils import code_to_fname, fname_to_code, exists_qlib_data
from qlib.constant import REG_CN as REGION_CN

CUR_DIR = Path(__file__).resolve().parent
sys.path.append(str(CUR_DIR.parent.parent))

from dump_bin import DumpDataUpdate
from data_collector.base import BaseCollector, BaseNormalize, BaseRun, Normalize
from data_collector.utils import (
    deco_retry,
    get_calendar_list,
    get_hs_stock_symbols,
    generate_minutes_calendar_from_daily,
)

from data_collector.rqdata.utils import calc_adjusted_price

class RQDataCollector(BaseCollector):
    def __init__(
        self,
        save_dir: [str, Path],
        start=None,
        end=None,
        interval="1d",
        max_workers=4,
        max_collector_count=2,
        delay=0,
        check_data_length: int = None,
        limit_nums: int = None,
        rqdata_username: str = None,
        rqdata_password: str = None,
        index_list: list = None,
    ):
        """
        参数
        ----------
        save_dir: str
            数据保存目录
        max_workers: int
            工作进程数，默认4
        max_collector_count: int
            默认2
        delay: float
            time.sleep(delay), 默认0
        interval: str
            频率，可选值 [1min, 1d], 默认1d
        start: str
            开始时间，默认None
        end: str
            结束时间，默认None
        check_data_length: int
            检查数据长度，默认None
        limit_nums: int
            用于调试，默认None
        rqdata_username: str
            RQData用户名
        rqdata_password: str
            RQData密码
        """
        try:
            if rqdata_username and rqdata_password and not rq.initialized():
                rq.init(username=rqdata_username, password=rqdata_password)
            else:
                rq.init()
        except Exception as e:
            raise RuntimeError(f"rqdata initialized failed: {e}")

        # 初始化缓存相关属性
        self.cache_data = pd.DataFrame()
        self.cache_file = Path(save_dir).parent / f"rqdata_cache_{interval}_{self.convert_datetime(start).strftime('%Y%m%d')}_{self.convert_datetime(end).strftime('%Y%m%d')}.pkl"
        if interval == self.INTERVAL_1min:
            self.rq_interval = '1m'
        elif interval == self.INTERVAL_1d:
            self.rq_interval = '1d'
        else:
            raise ValueError(f"interval error: {self.interval}")

        
        # 先初始化父类的基本属性
        super(RQDataCollector, self).__init__(
            save_dir=save_dir,
            start=start,
            end=end,
            interval=interval,
            max_workers=max_workers,
            max_collector_count=max_collector_count,
            delay=delay,
            check_data_length=check_data_length,
            limit_nums=limit_nums,
        )
        
        # 初始化日期时间
        self.init_datetime()
        self.index_list = index_list
        
    
    def init_datetime(self):
        # 确保时间类型一致
        self.start_datetime = pd.Timestamp(self.start_datetime)
        self.end_datetime = pd.Timestamp(self.end_datetime)
        
        if self.interval == self.INTERVAL_1min:
            default_start = pd.Timestamp(self.DEFAULT_START_DATETIME_1MIN)
            self.start_datetime = min(self.start_datetime, default_start)
        elif self.interval == self.INTERVAL_1d:
            pass
        else:
            raise ValueError(f"interval error: {self.interval}")

        # 确保时间戳没有时区信息
        self.start_datetime = self.start_datetime.tz_localize(None)
        self.end_datetime = self.end_datetime.tz_localize(None)

    @staticmethod
    def convert_datetime(dt: [pd.Timestamp, datetime.date, str]):
        try:
            dt = pd.Timestamp(dt)
        except ValueError as e:
            pass
        return dt

    def save_cache(self):
        """保存缓存数据到文件"""
        if not self.cache_data.empty:
            try:
                self.cache_data.to_pickle(self.cache_file)
                logger.info(f"Cache data saved to {self.cache_file}")
            except Exception as e:
                logger.warning(f"Failed to save cache: {e}")

    def load_cache(self):
        """从文件加载缓存数据"""
        if self.cache_file.exists():
            try:
                self.cache_data = pd.read_pickle(self.cache_file)
                logger.info(f"Cache data loaded from {self.cache_file}")
                return True
            except Exception as e:
                logger.warning(f"Failed to load cache: {e}")
        return False

    def get_data_from_cache(self, symbol, interval, start, end):
        """从缓存获取数据"""
        if self.cache_data.empty:
            if not self.load_cache():
                raise RuntimeError("Kline data is empty, please get all data from RQData first.")
        
        try:
            df = self.cache_data.xs(symbol, level=0)
            if df is not None and not df.empty:
                df = df.reset_index()
                df.rename(columns={"prev_close": "adjclose", "total_turnover": "money", 'datetime': "date"}, inplace=True, errors='ignore')
                df["symbol"] = symbol
                return df
            return None
        except Exception as e:
            logger.warning(f"get data error: {symbol}--{start}--{end}: {str(e)}")
            return None

    def get_data(
        self, symbol: str, interval: str, start_datetime: pd.Timestamp, end_datetime: pd.Timestamp
    ) -> pd.DataFrame:
        def _get_simple(start_, end_):
            resp = self.get_data_from_cache(
                symbol,
                interval=interval,
                start=start_,
                end=end_,
            )
            if resp is None or resp.empty:
                raise ValueError(
                    f"get data error: {symbol}--{start_}--{end_}" + "The stock may be delisted, please check"
                )
            return resp

        _result = None
        try:
            _result = _get_simple(start_datetime, end_datetime)
        except ValueError as e:
            pass
        return pd.DataFrame() if _result is None else _result

    def collector_data(self):
        """收集数据"""
        # 确保保存目录为空
        try:
            if self.save_dir.exists():
                import shutil
                shutil.rmtree(self.save_dir)
                logger.info(f"Cleared save directory: {self.save_dir}")
            self.save_dir.mkdir(parents=True, exist_ok=True)
        except Exception as e:
            logger.warning(f"Failed to clear save directory: {e}")
            raise RuntimeError(f"Failed to clear save directory: {e}")

        # 如果是主进程，先获取所有数据
        if not self.cache_file.exists():
            logger.info("First time running, getting all data from RQData...")
            super(RQDataCollector, self).collector_data()
            self.download_index_data()
        else:
            # 如果是子进程，直接使用缓存数据
            logger.info("Using cached data...")
            if not self.load_cache():
                raise RuntimeError("Failed to load cache data")
            super(RQDataCollector, self).collector_data()
            self.download_index_data()
        
        # 数据收集完成后删除缓存文件
        self.delete_cache_file()

    def delete_cache_file(self):
        """删除缓存文件"""
        try:
            if self.cache_file.exists():
                self.cache_file.unlink()
                logger.info(f"Cache file deleted: {self.cache_file}")
        except Exception as e:
            logger.warning(f"Failed to delete files: {e}")

    @abc.abstractmethod
    def download_index_data(self):
        """下载指数数据"""
        raise NotImplementedError("rewrite download_index_data")


class RQDataCollectorCN(RQDataCollector, ABC):
    def get_instrument_list(self):
        """获取A股股票列表"""
        logger.info("get HS stock symbols......")
        # all_instruments = rq.all_instruments(type="CS", date=self.end_datetime)
        all_instruments = rq.all_instruments(type="CS")
        active_symbols = all_instruments[all_instruments['status'] == 'Active']['order_book_id'].tolist()

        delisted_instruments = all_instruments[all_instruments['status'] == 'Delisted'].copy()
        delisted_instruments['de_listed_date'] = delisted_instruments['de_listed_date'].apply(pd.Timestamp)
        delisted_symbol = delisted_instruments[delisted_instruments['de_listed_date'] > self.start_datetime]
        delisted_symbols = delisted_symbol['order_book_id'].tolist()
        symbols = active_symbols + delisted_symbols
        
        logger.info(f"get {len(symbols)} symbols.")

        # 如果是主进程且缓存文件不存在，获取所有数据
        try:
            logger.info(f"Getting data for {len(symbols)} stocks from {self.start_datetime} to {self.end_datetime}...")
            self.cache_data = rq.get_price(
                symbols,
                start_date=self.start_datetime,
                end_date=self.end_datetime,
                frequency=self.rq_interval,
            )
            logger.info("Data retrieved successfully, saving to cache...")
            self.save_cache()
        except Exception as e:
            raise RuntimeError(f"get data error: {e}")

        return symbols

    def normalize_symbol(self, symbol):
        """标准化股票代码格式"""
        symbol_s = symbol.split(".")
        symbol = f"sh{symbol_s[0]}" if symbol_s[-1] == "XSHG" else f"sz{symbol_s[0]}"
        return symbol

    def download_index_data(self):
        """下载指数数据"""
        if len(self.index_list) == 0:
            return
        index_symbols = self.index_list  # CSI300, CSI500, CSI100
        logger.info(f"get bench data: {index_symbols}......")
        index_data = rq.get_price(index_symbols, start_date=self.start_datetime, end_date=self.end_datetime, frequency=self.rq_interval)
        for symbol in index_symbols:
            try:
                df = index_data.xs(symbol, level=0)
                df = df.reset_index()
                df.rename(columns={"prev_close": "adjclose", "total_turnover": "money", 'datetime': "date"}, inplace=True, errors='ignore')
                df["symbol"] = self.normalize_symbol(symbol)
                if not df.empty:
                    _path = self.save_dir.joinpath(f"{self.normalize_symbol(symbol)}.csv")
                    if _path.exists():
                        _old_df = pd.read_csv(_path)
                        df = pd.concat([_old_df, df], sort=False)
                    df.to_csv(_path, index=False)
            except Exception as e:
                logger.warning(f"get {symbol} error: {e}")
        logger.info(f"index data downloaded.")


class RQDataCollectorCN1d(RQDataCollectorCN):
    pass


class RQDataCollectorCN1min(RQDataCollectorCN):
    def get_instrument_list(self):
        symbols = super(RQDataCollectorCN1min, self).get_instrument_list()
        return symbols + self.index_list


class RQDataNormalize(BaseNormalize):
    COLUMNS = ["open", "close", "high", "low", "volume"]
    DAILY_FORMAT = "%Y-%m-%d"

    @staticmethod
    def calc_change(df: pd.DataFrame, last_close: float) -> pd.Series:
        """计算涨跌幅"""
        df = df.copy()
        _tmp_series = df["close"].fillna(method="ffill")
        _tmp_shift_series = _tmp_series.shift(1)
        if last_close is not None:
            _tmp_shift_series.iloc[0] = float(last_close)
        change_series = _tmp_series / _tmp_shift_series - 1
        return change_series

    @staticmethod
    def normalize_rqdata(
        df: pd.DataFrame,
        calendar_list: list = None,
        date_field_name: str = "date",
        symbol_field_name: str = "symbol",
        last_close: float = None,
    ):
        """标准化RQData数据"""
        if df.empty:
            return df
        symbol = df.loc[df[symbol_field_name].first_valid_index(), symbol_field_name]
        columns = copy.deepcopy(RQDataNormalize.COLUMNS)
        df = df.copy()
        df.set_index(date_field_name, inplace=True)
        df.index = pd.to_datetime(df.index)
        df.index = df.index.tz_localize(None)
        df = df[~df.index.duplicated(keep="first")]
        if calendar_list is not None:
            df = df.reindex(
                pd.DataFrame(index=calendar_list)
                .loc[
                    pd.Timestamp(df.index.min()).date() : pd.Timestamp(df.index.max()).date()
                    + pd.Timedelta(hours=23, minutes=59)
                ]
                .index
            )
        df.sort_index(inplace=True)
        df.loc[(df["volume"] <= 0) | np.isnan(df["volume"]), list(set(df.columns) - {symbol_field_name})] = np.nan

        df["change"] = RQDataNormalize.calc_change(df, last_close)
        columns += ["change"]
        df.loc[(df["volume"] <= 0) | np.isnan(df["volume"]), columns] = np.nan

        df[symbol_field_name] = symbol
        df.index.names = [date_field_name]
        return df.reset_index()

    def normalize(self, df: pd.DataFrame) -> pd.DataFrame:
        # normalize
        df = self.normalize_rqdata(df, self._calendar_list, self._date_field_name, self._symbol_field_name)
        # adjusted price
        df = self.adjusted_price(df)
        return df

    @abc.abstractmethod
    def adjusted_price(self, df: pd.DataFrame) -> pd.DataFrame:
        """调整价格"""
        raise NotImplementedError("rewrite adjusted_price")


class RQDataNormalize1d(RQDataNormalize, ABC):
    def adjusted_price(self, df: pd.DataFrame) -> pd.DataFrame:
        """调整日线数据价格"""
        if df.empty:
            return df
        df = df.copy()
        df.set_index(self._date_field_name, inplace=True)
        # RQData数据已经复权，factor设为1
        df["factor"] = 1
        df.index.names = [self._date_field_name]
        return df.reset_index()

    def normalize(self, df: pd.DataFrame) -> pd.DataFrame:
        df = super(RQDataNormalize1d, self).normalize(df)
        df = self._manual_adj_data(df)
        return df

    def _get_first_close(self, df: pd.DataFrame) -> float:
        """获取第一个有效收盘价"""
        df = df.loc[df["close"].first_valid_index() :]
        _close = df["close"].iloc[0]
        return _close

    def _manual_adj_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """手动调整数据：所有字段（除change外）都按照第一个交易日的收盘价标准化"""
        if df.empty:
            return df
        df = df.copy()
        df.sort_values(self._date_field_name, inplace=True)
        df = df.set_index(self._date_field_name)
        _close = self._get_first_close(df)
        for _col in df.columns:
            if _col in [self._symbol_field_name, "change"]:
                continue
            if _col == "volume":
                df[_col] = df[_col] * _close
            else:
                df[_col] = df[_col] / _close
        return df.reset_index()


class RQDataNormalize1min(RQDataNormalize, ABC):
    """标准化1分钟数据"""

    AM_RANGE = ("09:31:00", "11:30:00")
    PM_RANGE = ("13:01:00", "15:00:00")
    CONSISTENT_1d = True
    CALC_PAUSED_NUM = True

    def __init__(
        self, qlib_data_1d_dir: [str, Path], date_field_name: str = "date", symbol_field_name: str = "symbol", **kwargs
    ):
        super(RQDataNormalize1min, self).__init__(date_field_name, symbol_field_name)
        qlib.init(provider_uri=qlib_data_1d_dir)
        self.all_1d_data = D.features(D.instruments("all"), ["$paused", "$volume", "$factor", "$close"], freq="day")

    def _get_1d_calendar_list(self) -> Iterable[pd.Timestamp]:
        return list(D.calendar(freq="day"))

    @property
    def calendar_list_1d(self):
        calendar_list_1d = getattr(self, "_calendar_list_1d", None)
        if calendar_list_1d is None:
            calendar_list_1d = self._get_1d_calendar_list()
            setattr(self, "_calendar_list_1d", calendar_list_1d)
        return calendar_list_1d

    def generate_1min_from_daily(self, calendars: Iterable) -> pd.Index:
        return generate_minutes_calendar_from_daily(
            calendars, freq="1min", am_range=self.AM_RANGE, pm_range=self.PM_RANGE
        )

    def adjusted_price(self, df: pd.DataFrame) -> pd.DataFrame:
        """调整1分钟数据价格"""
        df = calc_adjusted_price(
            df=df,
            _date_field_name=self._date_field_name,
            _symbol_field_name=self._symbol_field_name,
            frequence="1min",
            consistent_1d=self.CONSISTENT_1d,
            calc_paused=self.CALC_PAUSED_NUM,
            _1d_data_all=self.all_1d_data,
            am_range=self.AM_RANGE,
            pm_range=self.PM_RANGE
        )
        return df


class RQDataNormalizeCN:
    def _get_calendar_list(self) -> Iterable[pd.Timestamp]:
        return get_calendar_list("ALL")


class RQDataNormalizeCN1d(RQDataNormalizeCN, RQDataNormalize1d):
    pass


class RQDataNormalizeCN1min(RQDataNormalizeCN, RQDataNormalize1min):
    def _get_calendar_list(self) -> Iterable[pd.Timestamp]:
        return self.generate_1min_from_daily(self.calendar_list_1d)

    def _get_1d_calendar_list(self) -> Iterable[pd.Timestamp]:
        return get_calendar_list("ALL")


class RQDataNormalize1dExtend(RQDataNormalize1d):
    def __init__(
        self, old_qlib_data_dir: [str, Path], date_field_name: str = "date", symbol_field_name: str = "symbol", **kwargs
    ):
        """初始化RQDataNormalize1dExtend

        参数
        ----------
        old_qlib_data_dir: str, Path
            需要更新的qlib数据目录，通常来自: https://github.com/microsoft/qlib/tree/main/scripts#download-cn-data
        date_field_name: str
            日期字段名，默认date
        symbol_field_name: str
            股票代码字段名，默认symbol
        """
        super(RQDataNormalize1dExtend, self).__init__(date_field_name, symbol_field_name)
        self.column_list = ["open", "high", "low", "close", "volume", "factor", "change"]
        self.old_qlib_data = self._get_old_data(old_qlib_data_dir)

    def _get_old_data(self, qlib_data_dir: [str, Path]):
        """获取旧的qlib数据

        参数
        ----------
        qlib_data_dir: str, Path
            qlib数据目录

        返回
        ----------
        pd.DataFrame
            包含所有股票数据的DataFrame
        """
        qlib_data_dir = str(Path(qlib_data_dir).expanduser().resolve())
        qlib.init(provider_uri=qlib_data_dir, expression_cache=None, dataset_cache=None)
        df = D.features(D.instruments("all"), ["$" + col for col in self.column_list])
        df.columns = self.column_list
        
        # 确保日期格式正确，处理MultiIndex
        df.index = pd.MultiIndex.from_arrays([
            df.index.get_level_values(0),  # instrument
            pd.to_datetime(df.index.get_level_values(1)).date  # datetime
        ], names=df.index.names)
        return df

    def normalize(self, df: pd.DataFrame) -> pd.DataFrame:
        """标准化数据

        参数
        ----------
        df: pd.DataFrame
            需要标准化的数据

        返回
        ----------
        pd.DataFrame
            标准化后的数据
        """
        df = super(RQDataNormalize1dExtend, self).normalize(df)
        
        # 确保日期格式正确
        df[self._date_field_name] = pd.to_datetime(df[self._date_field_name]).dt.date
        df.set_index(self._date_field_name, inplace=True)
        
        symbol_name = df[self._symbol_field_name].iloc[0]
        old_symbol_list = self.old_qlib_data.index.get_level_values("instrument").unique().to_list()
        if str(symbol_name).upper() not in old_symbol_list:
            return df.reset_index()
            
        old_df = self.old_qlib_data.loc[str(symbol_name).upper()]
        latest_date = old_df.index[-1]
        df = df.loc[latest_date:]
        new_latest_data = df.iloc[0]
        old_latest_data = old_df.loc[latest_date]
        
        for col in self.column_list[:-1]:
            if col == "volume":
                df[col] = df[col] / (new_latest_data[col] / old_latest_data[col])
            else:
                df[col] = df[col] * (old_latest_data[col] / new_latest_data[col])
                
        return df.drop(df.index[0]).reset_index()

class RQDataNormalizeCN1dExtend(RQDataNormalizeCN, RQDataNormalize1dExtend):
    pass


class Run(BaseRun):
    def __init__(self, source_dir=None, normalize_dir=None, max_workers=1, interval="1d", region=REGION_CN):
        """
        参数
        ----------
        source_dir: str
            原始数据保存目录，默认 "Path(__file__).parent/source"
        normalize_dir: str
            标准化数据保存目录，默认 "Path(__file__).parent/normalize"
        max_workers: int
            并发数，默认1；收集数据时建议max_workers设为1
        interval: str
            频率，可选值 [1min, 1d], 默认1d
        region: str
            地区，目前只支持"CN"
        """
        super().__init__(source_dir, normalize_dir, max_workers, interval)
        self.region = region
        self.rqdata_username = None
        self.rqdata_password = None
        self.index_list = []

    @property
    def collector_class_name(self):
        return f"RQDataCollector{self.region.upper()}{self.interval}"

    @property
    def normalize_class_name(self):
        return f"RQDataNormalize{self.region.upper()}{self.interval}"

    @property
    def default_base_dir(self) -> [Path, str]:
        return CUR_DIR

    def download_data(
        self,
        max_collector_count=2,
        delay=0,
        start=None,
        end=None,
        check_data_length=None,
        limit_nums=None,
        rqdata_username=None,
        rqdata_password=None, 
        index=None,
        **kwargs
    ):
        """从RQData下载数据

        参数
        ----------
        max_collector_count: int
            默认2
        delay: float
            time.sleep(delay), 默认0.5
        start: str
            开始时间，默认 "2000-01-01"；闭区间(包含start)
        end: str
            结束时间，默认 ``pd.Timestamp(datetime.datetime.now() + pd.Timedelta(days=1))``；开区间(不包含end)
        check_data_length: int
            检查数据长度，如果非None且大于0，则每个股票的数据长度大于等于此值才认为完整，否则会重新获取，最多获取(max_collector_count)次。默认None。
        limit_nums: int
            用于调试，默认None
        rqdata_username: str
            RQData用户名
        rqdata_password: str
            RQData密码
        index: str
            指数列表，用逗号分隔，如"CSI300,CSI500"
        **kwargs: dict
            其他传递给父类的参数

        示例
        ---------
            # 获取日线数据
            $ python collector.py download_data --source_dir ~/.qlib/stock_data/source --region CN --start 2020-11-01 --end 2020-11-10 --delay 0.1 --interval 1d
            # 获取1分钟数据
            $ python collector.py download_data --source_dir ~/.qlib/stock_data/source --region CN --start 2020-11-01 --end 2020-11-10 --delay 0.1 --interval 1m
        """
        if self.interval == "1d" and pd.Timestamp(end) > pd.Timestamp(datetime.datetime.now().strftime("%Y-%m-%d")):
            raise ValueError(f"end_date: {end} is greater than the current date.")
        
        # 存储RQData相关参数
        self.rqdata_username = rqdata_username
        self.rqdata_password = rqdata_password
        self.index_list = index.split(",") if index is not None else []

        # 准备传递给父类的参数
        parent_kwargs = {
            "max_collector_count": max_collector_count,
            "delay": delay,
            "start": start,
            "end": end,
            "check_data_length": check_data_length,
            "limit_nums": limit_nums,
            "rqdata_username": rqdata_username,
            "rqdata_password": rqdata_password,
            "index_list": self.index_list,
            **kwargs
        }

        # 调用父类方法，传入所有参数
        super(Run, self).download_data(**parent_kwargs)

    def normalize_data(
        self,
        date_field_name: str = "date",
        symbol_field_name: str = "symbol",
        end_date: str = None,
        qlib_data_1d_dir: str = None,
    ):
        """标准化数据

        参数
        ----------
        date_field_name: str
            日期字段名，默认date
        symbol_field_name: str
            股票代码字段名，默认symbol
        end_date: str
            如果非None，则标准化保存的最后日期(包含end_date)；如果None，则忽略此参数；默认None
        qlib_data_1d_dir: str
            如果interval==1min，qlib_data_1d_dir不能为None，标准化1分钟数据需要使用日线数据

        示例
        ---------
            $ python collector.py normalize_data --source_dir ~/.qlib/stock_data/source --normalize_dir ~/.qlib/stock_data/normalize --region cn --interval 1d
            $ python collector.py normalize_data --qlib_data_1d_dir ~/.qlib/qlib_data/cn_data --source_dir ~/.qlib/stock_data/source_cn_1min --normalize_dir ~/.qlib/stock_data/normalize_cn_1min --region CN --interval 1min
        """
        if self.interval.lower() == "1min":
            if qlib_data_1d_dir is None or not Path(qlib_data_1d_dir).expanduser().exists():
                raise ValueError(
                    "If normalize 1min, the qlib_data_1d_dir parameter must be set: --qlib_data_1d_dir <user qlib 1d data >"
                )
        super(Run, self).normalize_data(
            date_field_name, symbol_field_name, end_date=end_date, qlib_data_1d_dir=qlib_data_1d_dir
        )
    
    def normalize_data_1d_extend(
        self, old_qlib_data_dir, date_field_name: str = "date", symbol_field_name: str = "symbol"
    ):
        """标准化数据扩展；扩展RQData qlib数据

        注意事项
        -----
            扩展RQData qlib数据的步骤：

                1. 下载qlib数据: https://github.com/microsoft/qlib/tree/main/scripts#download-cn-data; 保存到 <dir1>

                2. 收集源数据: https://github.com/microsoft/qlib/tree/main/scripts/data_collector/rqdata#collector-data; 保存到 <dir2>

                3. 标准化新的源数据(来自步骤2): python scripts/data_collector/rqdata/collector.py normalize_data_1d_extend --old_qlib_dir <dir1> --source_dir <dir2> --normalize_dir <dir3> --region CN --interval 1d

                4. 转储数据: python scripts/dump_bin.py dump_update --csv_path <dir3> --qlib_dir <dir1> --freq day --date_field_name date --symbol_field_name symbol --exclude_fields symbol,date

                5. 更新指数成分股(例如: csi300): python scripts/data_collector/cn_index/collector.py --index_name CSI300 --qlib_dir <dir1> --method parse_instruments

        参数
        ----------
        old_qlib_data_dir: str
            需要更新的qlib数据目录，通常来自: https://github.com/microsoft/qlib/tree/main/scripts#download-cn-data
        date_field_name: str
            日期字段名，默认date
        symbol_field_name: str
            股票代码字段名，默认symbol

        示例
        ---------
            $ python collector.py normalize_data_1d_extend --old_qlib_dir ~/.qlib/qlib_data/cn_data --source_dir ~/.qlib/stock_data/source --normalize_dir ~/.qlib/stock_data/normalize --region CN --interval 1d
        """
        _class = getattr(self._cur_module, f"{self.normalize_class_name}Extend")
        rc = Normalize(
            source_dir=self.source_dir,
            target_dir=self.normalize_dir,
            normalize_class=_class,
            max_workers=self.max_workers,
            date_field_name=date_field_name,
            symbol_field_name=symbol_field_name,
            old_qlib_data_dir=old_qlib_data_dir,
        )
        rc.normalize()

    def update_data_to_bin(
        self,
        qlib_data_1d_dir: str,
        end_date: str = None,
        check_data_length: int = None,
        delay: float = 0,
        exists_skip: bool = False,
        rqdata_username: str = None,
        rqdata_password: str = None,
        index: str = None,
    ):
        """更新RQData数据到bin文件

        参数
        ----------
        qlib_data_1d_dir: str
            需要更新的qlib数据目录，通常来自: https://github.com/microsoft/qlib/tree/main/scripts#download-cn-data
        end_date: str
            结束日期，默认 ``pd.Timestamp(trading_date + pd.Timedelta(days=1))``；开区间(不包含end)
        check_data_length: int
            检查数据长度，如果非None且大于0，则每个股票的数据长度大于等于此值才认为完整，否则会重新获取，最多获取(max_collector_count)次。默认None。
        delay: float
            time.sleep(delay), 默认0
        exists_skip: bool
            如果目标目录数据已存在，是否跳过获取数据，默认False
        rqdata_username: str
            RQData用户名
        rqdata_password: str
            RQData密码
        index: str
            指数列表，用逗号分隔，如"CSI300,CSI500"

        注意事项
        -----
            如果qlib_data_dir中的数据不完整，将会用np.nan填充到前一个交易日的trading_date

        示例
        -------
            $ python collector.py update_data_to_bin --qlib_data_1d_dir <user data dir> --end_date <end date>
        """
        if self.interval.lower() != "1d":
            logger.warning(f"目前仅支持1d数据更新: --interval 1d")

        # 下载qlib 1d数据
        qlib_data_1d_dir = str(Path(qlib_data_1d_dir).expanduser().resolve())
        if not exists_qlib_data(qlib_data_1d_dir):
            GetData().qlib_data(
                target_dir=qlib_data_1d_dir, interval=self.interval, region=self.region, exists_skip=exists_skip
            )

        # 获取开始/结束日期
        calendar_df = pd.read_csv(Path(qlib_data_1d_dir).joinpath("calendars/day.txt"))
        trading_date = (pd.Timestamp(calendar_df.iloc[-1, 0]) - pd.Timedelta(days=1)).strftime("%Y-%m-%d")

        if end_date is None:
            end_date = (pd.Timestamp(trading_date) + pd.Timedelta(days=1)).strftime("%Y-%m-%d")

        # 从RQData下载数据
        # 注意：从RQData下载数据时，建议max_workers设为1
        self.download_data(
            delay=delay, 
            start=trading_date, 
            end=end_date, 
            check_data_length=check_data_length,
            rqdata_username=rqdata_username,
            rqdata_password=rqdata_password,
            index=index
        )
        
        # 注意：这里可以使用更大的max_workers设置来加快速度
        self.max_workers = (
            max(multiprocessing.cpu_count() - 2, 1)
            if self.max_workers is None or self.max_workers <= 1
            else self.max_workers
        )
        
        # 标准化数据
        self.normalize_data_1d_extend(qlib_data_1d_dir)

        # 转储为bin文件
        _dump = DumpDataUpdate(
            csv_path=self.normalize_dir,
            qlib_dir=qlib_data_1d_dir,
            exclude_fields="symbol,date",
            max_workers=self.max_workers,
        )
        _dump.dump()

        # 解析指数
        _region = self.region.lower()
        if _region != "cn":
            logger.warning(f"不支持的地区: region={_region}，将忽略成分股下载")
            return
        
        if index is not None:
            index_list = index.split(",")
        else:
            index_list = []

        
        # get_instruments = getattr(
        #     importlib.import_module(f"data_collector.rqdata_index.collector"), "get_instruments"
        # )
        for index_name in index_list:
            try:
                # get_instruments(index_name=index_name, qlib_dir=qlib_data_1d_dir, method="parse_instruments", market_index="rqdata_index")
                _cur_module = importlib.import_module("data_collector.{}.collector".format("rqdata_index"))
                obj = getattr(_cur_module, f"RQDataIndex")(
                    qlib_dir=qlib_data_1d_dir, index_name=index_name, freq="day", rqdata_username=rqdata_username, rqdata_password=rqdata_password
                )
                getattr(obj, "parse_instruments")()
            except Exception as e:
                logger.warning(f"更新{index_name}成分股失败: {e}")

    


if __name__ == "__main__":
    fire.Fire(Run) 