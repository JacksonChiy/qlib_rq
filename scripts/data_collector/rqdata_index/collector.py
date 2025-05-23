# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

import abc
import sys
from pathlib import Path
from typing import List, Dict, Optional
from datetime import datetime

import fire
import pandas as pd
import rqdatac as rq
import requests
from bs4 import BeautifulSoup
from tqdm import tqdm
from loguru import logger

CUR_DIR = Path(__file__).resolve().parent
sys.path.append(str(CUR_DIR.parent.parent))

from data_collector.index import IndexBase
from data_collector.utils import get_calendar_list, get_trading_date_by_shift
from data_collector.utils import get_instruments

RQ_INDEX_URL = "https://www.ricequant.com/doc/rqdata/python/indices-dictionary.html"

class RQDataIndex(IndexBase):
    def __init__(
        self,
        index_name: str,
        qlib_dir: [str, Path] = None,
        freq: str = "day",
        request_retry: int = 5,
        retry_sleep: int = 3,
        rqdata_username: str = None,
        rqdata_password: str = None,
    ):
        super(RQDataIndex, self).__init__(
            index_name=index_name, qlib_dir=qlib_dir, freq=freq, request_retry=request_retry, retry_sleep=retry_sleep
        )
        # 初始化RQData
        try:
            if rqdata_username and rqdata_password and not rq.initialized():
                rq.init(username=rqdata_username, password=rqdata_password)
            else:
                rq.init()
        except Exception as e:
            raise RuntimeError(f"rqdata initialized failed: {e}")
        # 初始化指数信息
        self._index_info = None
        self.index_code = self.index_name
        self.index_name = self.index_name.split('.')[0]

    @property
    def index_info(self) -> Dict[str, str]:
        """获取指数的详细信息，包括发布日期和退市日期

        Returns
        -------
        Dict[str, str]:
            包含指数信息的字典，包括：
            - publish_date: 发布日期
            - delist_date: 退市日期
        """
        if self._index_info is None:
            try:
                # 获取网页内容
                url = RQ_INDEX_URL
                response = requests.get(url)
                response.raise_for_status()
                
                # 解析HTML
                soup = BeautifulSoup(response.text, 'html.parser')
                
                # 查找包含指数信息的表格
                table = soup.find('table')
                if table:
                    # 遍历表格行
                    for row in table.find_all('tr')[1:]:  # 跳过表头
                        cols = row.find_all('td')
                        if len(cols) >= 5:  # 确保有足够的列
                            code = cols[0].text.strip()
                            if code == self.index_code:
                                self._index_info = {
                                    'publish_date': cols[3].text.strip(),
                                    'delist_date': cols[4].text.strip()
                                }
                                break
                
                if not self._index_info:
                    logger.warning(f"未找到指数 {self.index_code} 的信息")
                    self._index_info = {
                        'publish_date': '0000-00-00',
                        'delist_date': '0000-00-00'
                    }
                    
            except Exception as e:
                logger.error(f"获取指数信息时发生错误: {str(e)}")
                self._index_info = {
                    'publish_date': '0000-00-00',
                    'delist_date': '0000-00-00'
                }
                
        return self._index_info

    @property
    def publish_date(self) -> pd.Timestamp:
        """获取指数的发布日期

        Returns
        -------
        pd.Timestamp:
            指数的发布日期
        """
        date_str = self.index_info['publish_date']
        if date_str == '0000-00-00':
            raise ValueError(f"指数 {self.index_code} 的发布日期为空")
        return pd.Timestamp(date_str)

    @property
    def delist_date(self) -> pd.Timestamp:
        """获取指数的退市日期

        Returns
        -------
        pd.Timestamp:
            指数的退市日期，如果未退市则返回默认值
        """
        date_str = self.index_info['delist_date']
        if date_str == '0000-00-00':
            return pd.Timestamp('2099-12-31')
        return pd.Timestamp(date_str)

    @property
    def calendar_list(self) -> List[pd.Timestamp]:
        """获取历史交易日历

        Returns
        -------
            calendar list of pd.Timestamp
        """
        _calendar = getattr(self, "_calendar_list", None)
        if not _calendar:
            end_date = datetime.today().strftime("%Y-%m-%d") if self.delist_date > datetime.today() else self.delist_date.strftime("%Y-%m-%d")
            _calendar = [pd.Timestamp(dt) for dt in rq.get_trading_dates(start_date=self.publish_date, end_date=end_date)]
            setattr(self, "_calendar_list", _calendar)
        return _calendar

    # @property
    # @abc.abstractmethod
    # def bench_start_date(self) -> pd.Timestamp:
    #     """
    #     Returns
    #     -------
    #         index start date
    #     """
    #     raise NotImplementedError("rewrite bench_start_date")

    # @property
    # def index_code(self) -> str:
    #     """
    #     Returns
    #     -------
    #         index code in RQData format (e.g. "000300.XSHG" for CSI300)
    #     """
    #     return self.index_name

    def format_datetime(self, inst_df: pd.DataFrame) -> pd.DataFrame:
        """格式化日期时间

        Parameters
        ----------
        inst_df: pd.DataFrame
            inst_df.columns = [self.SYMBOL_FIELD_NAME, self.START_DATE_FIELD, self.END_DATE_FIELD]

        Returns
        -------
            pd.DataFrame with formatted datetime
        """
        if self.freq != "day":
            inst_df[self.START_DATE_FIELD] = inst_df[self.START_DATE_FIELD].apply(
                lambda x: (pd.Timestamp(x) + pd.Timedelta(hours=9, minutes=30)).strftime("%Y-%m-%d %H:%M:%S")
            )
            inst_df[self.END_DATE_FIELD] = inst_df[self.END_DATE_FIELD].apply(
                lambda x: (pd.Timestamp(x) + pd.Timedelta(hours=15, minutes=0)).strftime("%Y-%m-%d %H:%M:%S")
            )
        else:
            inst_df[self.START_DATE_FIELD] = inst_df[self.START_DATE_FIELD].apply(
                lambda x: pd.Timestamp(x).strftime("%Y-%m-%d")
            )
            inst_df[self.END_DATE_FIELD] = inst_df[self.END_DATE_FIELD].apply(
                lambda x: pd.Timestamp(x).strftime("%Y-%m-%d")
            )
        return inst_df

    def normalize_symbol(self, symbol: str) -> str:
        """标准化股票代码格式

        Parameters
        ----------
        symbol: str
            RQData格式的股票代码 (e.g. "000001.XSHE")

        Returns
        -------
            str: 标准化后的股票代码 (e.g. "SZ000001")
        """
        if symbol.endswith(".XSHG"):
            return f"SH{symbol.split('.')[0]}"
        elif symbol.endswith(".XSHE"):
            return f"SZ{symbol.split('.')[0]}"
        return symbol

    # def get_new_companies(self) -> pd.DataFrame:
    #     """获取最新的指数成分股

    #     Returns
    #     -------
    #         pd.DataFrame:
    #             symbol     start_date    end_date
    #             SH600000   2000-01-01    2099-12-31

    #         dtypes:
    #             symbol: str
    #             start_date: pd.Timestamp
    #             end_date: pd.Timestamp
    #     """
    #     logger.info(f"获取{self.index_name}最新成分股...")
    #     today = pd.Timestamp.now().normalize()
        
    #     # 获取当前成分股
    #     constituents_dict = rq.index_components(
    #         self.index_code,
    #         start_date=today.strftime("%Y%m%d"),
    #         end_date=today.strftime("%Y%m%d")
    #     )
        
    #     if not constituents_dict:
    #         raise ValueError(f"无法获取{self.index_name}的成分股数据")
            
    #     # 获取成分股的上市日期
    #     df = pd.DataFrame()
    #     constituents = list(constituents_dict.values())[0]  # 获取当天的成分股列表
    #     df[self.SYMBOL_FIELD_NAME] = [self.normalize_symbol(s) for s in constituents]
    #     df[self.START_DATE_FIELD] = self.bench_start_date
    #     df[self.END_DATE_FIELD] = self.DEFAULT_END_DATE
        
    #     logger.info(f"获取{self.index_name}最新成分股完成")
    #     return df

    # def get_changes(self) -> pd.DataFrame:
    #     """获取成分股变更历史

    #     Returns
    #     -------
    #         pd.DataFrame:
    #             symbol      date        type
    #             SH600000  2019-11-11    add
    #             SH600000  2020-11-10    remove
    #         dtypes:
    #             symbol: str
    #             date: pd.Timestamp
    #             type: str, value from ["add", "remove"]
    #     """
    #     logger.info(f"获取{self.index_name}成分股变更历史...")
        
    #     # 获取历史交易日历
    #     trading_dates = self.calendar_list
    #     trading_dates = trading_dates[trading_dates >= self.bench_start_date]
        
    #     # 使用日期范围查询获取成分股历史
    #     start_date = trading_dates[0].strftime("%Y%m%d")
    #     end_date = trading_dates[-1].strftime("%Y%m%d")
        
    #     # 使用日期范围查询获取成分股历史数据
    #     constituents_dict = rq.index_components(
    #         self.index_code,
    #         start_date=start_date,
    #         end_date=end_date
    #     )
        
    #     if not constituents_dict:
    #         raise ValueError(f"无法获取{self.index_name}的成分股历史数据")
            
    #     # 转换为标准格式并记录变更
    #     changes = []
    #     last_constituents = set()
        
    #     # 按日期排序处理成分股变更
    #     for date, constituents in sorted(constituents_dict.items()):
    #         # 转换为标准格式
    #         constituents = {self.normalize_symbol(s) for s in constituents}
            
    #         if last_constituents:
    #             # 找出新增和删除的成分股
    #             added = constituents - last_constituents
    #             removed = last_constituents - constituents
                
    #             # 记录变更
    #             for symbol in added:
    #                 changes.append({
    #                     self.SYMBOL_FIELD_NAME: symbol,
    #                     self.DATE_FIELD_NAME: pd.Timestamp(date),
    #                     self.CHANGE_TYPE_FIELD: self.ADD
    #                 })
    #             for symbol in removed:
    #                 changes.append({
    #                     self.SYMBOL_FIELD_NAME: symbol,
    #                     self.DATE_FIELD_NAME: pd.Timestamp(date),
    #                     self.CHANGE_TYPE_FIELD: self.REMOVE
    #                 })
            
    #         last_constituents = constituents
        
    #     df = pd.DataFrame(changes)
    #     if not df.empty:
    #         df = df.sort_values(self.DATE_FIELD_NAME)
            
    #     logger.info(f"获取{self.index_name}成分股变更历史完成")
    #     return df

    def get_index_components(self) -> pd.DataFrame:
        """获取指数成分股历史列表

        Returns
        -------
            pd.DataFrame:
                symbol     start_date    end_date
                SH600000   2000-01-01    2005-12-30    # 结束日期为调出前一日
                SH600000   2007-01-01    2010-12-30    # 结束日期为调出前一日
                SH600000   2015-01-01    2099-12-31    # 仍在指数中，使用默认结束日期

            dtypes:
                symbol: str
                start_date: pd.Timestamp
                end_date: pd.Timestamp
        """
        logger.info(f"获取{self.index_name}历史成分股...")
        today = pd.Timestamp.now().normalize()
        default_end_date = pd.Timestamp("2099-12-31")
        
        # 获取历史成分股数据
        constituents_dict = rq.index_components(
            self.index_code,
            start_date=self.publish_date,
            end_date=today.strftime("%Y%m%d")
        )
        
        if not constituents_dict:
            raise ValueError(f"无法获取{self.index_name}的成分股数据")
            
        # 按日期排序处理成分股数据
        sorted_dates = sorted(constituents_dict.keys())
        if not sorted_dates:
            return pd.DataFrame(columns=[self.SYMBOL_FIELD_NAME, self.START_DATE_FIELD, self.END_DATE_FIELD])
            
        # 记录每个股票的时间段
        symbol_periods = {}  # {symbol: [(start_date, end_date), ...]}
        prev_constituents = set()  # 前一天的成分股集合
        prev_date = None  # 前一个交易日
        
        # 处理第一个交易日
        first_date = pd.Timestamp(sorted_dates[0])
        first_constituents = {self.normalize_symbol(s) for s in constituents_dict[first_date]}
        for symbol in first_constituents:
            symbol_periods[symbol] = [(first_date, first_date)]
        prev_constituents = first_constituents
        prev_date = first_date
        
        # 处理后续交易日
        for curr_date in sorted_dates[1:]:
            curr_date = pd.Timestamp(curr_date)
            curr_constituents = {self.normalize_symbol(s) for s in constituents_dict[curr_date]}
            
            # 找出新增和删除的成分股
            added = curr_constituents - prev_constituents
            removed = prev_constituents - curr_constituents
            
            # 处理新增的成分股
            for symbol in added:
                if symbol not in symbol_periods:
                    symbol_periods[symbol] = []
                symbol_periods[symbol].append((curr_date, curr_date))
            
            # 处理删除的成分股，使用前一个交易日作为结束日期
            for symbol in removed:
                if symbol in symbol_periods:
                    # 更新最后一个时间段的结束日期为前一个交易日
                    last_period = symbol_periods[symbol][-1]
                    symbol_periods[symbol][-1] = (last_period[0], prev_date)
            
            # 更新持续存在的成分股的结束日期
            for symbol in curr_constituents - added - removed:
                if symbol in symbol_periods:
                    last_period = symbol_periods[symbol][-1]
                    # 如果是最后一个交易日，使用默认结束日期
                    end_date = default_end_date if curr_date == today else curr_date
                    symbol_periods[symbol][-1] = (last_period[0], end_date)
            
            prev_constituents = curr_constituents
            prev_date = curr_date
        
        # 构建DataFrame
        records = []
        for symbol, periods in symbol_periods.items():
            for start_date, end_date in periods:
                # 如果结束日期是今天，使用默认结束日期
                if end_date == today:
                    end_date = default_end_date
                records.append({
                    self.SYMBOL_FIELD_NAME: symbol,
                    self.START_DATE_FIELD: start_date,
                    self.END_DATE_FIELD: end_date
                })
        
        df = pd.DataFrame(records)
        if not df.empty:
            df = df.sort_values(by=[self.SYMBOL_FIELD_NAME, self.START_DATE_FIELD])
        
        logger.info(f"获取{self.index_name}历史成分股完成，共{len(df)}条记录")
        return df
    
    def parse_instruments(self) -> pd.DataFrame:
        inst_df = self.get_index_components()
        inst_df = self.format_datetime(inst_df)
        inst_df.to_csv(
            self.instruments_dir.joinpath(f"{self.index_name.lower()}.txt"), sep="\t", index=False, header=None
        )
        logger.info(f"parse {self.index_name.lower()} companies finished.")


if __name__ == "__main__":
    fire.Fire(get_instruments) 