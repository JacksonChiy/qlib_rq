# RQData数据收集器使用指南

本文档详细介绍了如何使用QLib的RQData数据收集器来获取和处理股票数据。

## 目录
- [功能概述](#功能概述)
- [环境要求](#环境要求)
- [数据收集器类型](#数据收集器类型)
- [使用方法](#使用方法)
- [数据格式说明](#数据格式说明)
- [注意事项](#注意事项)
- [命令行使用方法](#命令行使用方法)

## 功能概述

RQData数据收集器提供了以下主要功能：
1. 从RQData获取股票日线数据（1d）
2. 从RQData获取股票分钟线数据（1min）
3. 从RQData获取指数成分股数据
4. 数据标准化和预处理
5. 数据缓存管理
6. 数据更新和扩展

## 环境要求

- Python 3.7+
- RQData账号（需要有效的用户名和密码）
- 依赖包：
  ```bash
  pip install rqdatac pandas numpy loguru fire requests beautifulsoup4
  ```

## 数据收集器类型

### 1. 股票数据收集器 (rqdata/collector.py)

#### RQDataCollectorCN1d
用于收集中国A股日线数据，主要特点：
- 支持日线数据获取
- 自动处理复权数据
- 支持数据缓存
- 支持多进程并行下载

#### RQDataCollectorCN1min
用于收集中国A股分钟线数据，主要特点：
- 支持分钟线数据获取
- 自动处理交易时段（上午9:31-11:30，下午13:01-15:00）
- 支持与日线数据对齐
- 支持数据缓存

### 2. 指数数据收集器 (rqdata_index/collector.py)

#### RQDataIndex
用于收集指数成分股数据，主要特点：
- 支持获取指数成分股列表
- 自动获取指数发布日期和退市日期
- 支持日线和分钟线频率
- 自动处理交易日历

## 使用方法

### 1. 初始化数据收集器

```python
from scripts.data_collector.rqdata.collector import Run

# 初始化收集器
collector = Run(
    source_dir="path/to/source",  # 原始数据保存目录
    normalize_dir="path/to/normalize",  # 标准化数据保存目录
    max_workers=4,  # 并行工作进程数
    interval="1d",  # 数据频率：1d 或 1min
    region="CN"  # 市场区域
)
```

### 2. 下载数据

```python
# 下载日线数据
collector.download_data(
    max_collector_count=2,  # 最大收集器数量
    delay=0,  # 请求延迟
    start="2020-01-01",  # 开始日期
    end="2023-12-31",  # 结束日期
    rqdata_username="your_username",  # RQData用户名
    rqdata_password="your_password",  # RQData密码
    check_data_length=None,  # 数据长度检查
    limit_nums=None  # 限制下载的股票数量（用于测试）
)
```

### 3. 标准化数据

```python
# 标准化日线数据
collector.normalize_data(
    date_field_name="date",
    symbol_field_name="symbol",
    end_date="2023-12-31",
    qlib_data_1d_dir="path/to/qlib_data_1d"
)

# 标准化分钟线数据
collector.normalize_data(
    date_field_name="date",
    symbol_field_name="symbol",
    end_date="2023-12-31",
    qlib_data_1d_dir="path/to/qlib_data_1d"
)
```

### 4. 更新数据

```python
# 更新数据到二进制格式
collector.update_data_to_bin(
    qlib_data_1d_dir="path/to/qlib_data_1d",
    end_date="2023-12-31",
    check_data_length=None,
    delay=0,
    exists_skip=False,
    rqdata_username="your_username",
    rqdata_password="your_password"
)
```

### 5. 获取指数成分股

```python
from scripts.data_collector.rqdata_index.collector import RQDataIndex

# 初始化指数收集器
index_collector = RQDataIndex(
    index_name="000300.XSHG",  # 指数代码
    qlib_dir="path/to/qlib_data",
    freq="day",  # 数据频率：day 或 min
    rqdata_username="your_username",
    rqdata_password="your_password"
)

# 获取指数成分股
components = index_collector.get_index_components()
```

## 数据格式说明

### 日线数据格式
- date: 交易日期
- symbol: 股票代码
- open: 开盘价
- close: 收盘价
- high: 最高价
- low: 最低价
- volume: 成交量
- money: 成交额
- adjclose: 复权收盘价

### 分钟线数据格式
- date: 交易日期时间
- symbol: 股票代码
- open: 开盘价
- close: 收盘价
- high: 最高价
- low: 最低价
- volume: 成交量
- money: 成交额
- adjclose: 复权收盘价

## 注意事项

### 1. 数据获取
- 需要有效的RQData账号
- 建议使用数据缓存功能提高效率
- 注意控制请求频率，避免触发限制

### 2. 数据标准化
- 日线数据会自动进行复权处理
- 分钟线数据会自动对齐交易时段
- 注意检查数据完整性

### 3. 性能优化
- 合理设置max_workers参数
- 使用数据缓存减少重复请求
- 适当设置delay参数避免请求过快

### 4. 错误处理
- 程序会自动重试失败的请求
- 日志会记录详细的错误信息
- 建议定期检查日志文件

### 5. 数据更新
- 更新前建议备份原有数据
- 注意检查数据一致性
- 建议在非交易时段进行更新

## 命令行使用方法

RQData数据收集器支持通过命令行直接运行，无需编写Python代码。以下是常用的命令行操作：

### 1. 下载数据

```bash
# 下载日线数据
python scripts/data_collector/rqdata/collector.py download_data \
    --source_dir "path/to/source" \
    --normalize_dir "path/to/normalize" \
    --max_workers 4 \
    --interval "1d" \
    --start "2020-01-01" \
    --end "2023-12-31" \
    --rqdata_username "your_username" \
    --rqdata_password "your_password" \
    --max_collector_count 2 \
    --delay 0

# 下载分钟线数据
python scripts/data_collector/rqdata/collector.py download_data \
    --source_dir "path/to/source" \
    --normalize_dir "path/to/normalize" \
    --max_workers 4 \
    --interval "1min" \
    --start "2020-01-01" \
    --end "2023-12-31" \
    --rqdata_username "your_username" \
    --rqdata_password "your_password" \
    --max_collector_count 2 \
    --delay 0
```

### 2. 标准化数据

```bash
# 标准化日线数据
python scripts/data_collector/rqdata/collector.py normalize_data \
    --source_dir "path/to/source" \
    --normalize_dir "path/to/normalize" \
    --date_field_name "date" \
    --symbol_field_name "symbol" \
    --end_date "2023-12-31" \
    --qlib_data_1d_dir "path/to/qlib_data_1d"

# 标准化分钟线数据
python scripts/data_collector/rqdata/collector.py normalize_data \
    --source_dir "path/to/source" \
    --normalize_dir "path/to/normalize" \
    --date_field_name "date" \
    --symbol_field_name "symbol" \
    --end_date "2023-12-31" \
    --qlib_data_1d_dir "path/to/qlib_data_1d"
```

### 3. 更新数据

```bash
# 更新数据到二进制格式
python scripts/data_collector/rqdata/collector.py update_data_to_bin \
    --qlib_data_1d_dir "path/to/qlib_data_1d" \
    --end_date "2023-12-31" \
    --delay 0 \
    --exists_skip False \
    --rqdata_username "your_username" \
    --rqdata_password "your_password"
```

### 4. 获取指数成分股

```bash
# 获取指数成分股数据
python scripts/data_collector/rqdata_index/collector.py get_index_components \
    --index_name "000300.XSHG" \
    --qlib_dir "path/to/qlib_data" \
    --freq "day" \
    --rqdata_username "your_username" \
    --rqdata_password "your_password"
```

### 5. 命令行参数说明

#### 通用参数
- `--source_dir`: 原始数据保存目录
- `--normalize_dir`: 标准化数据保存目录
- `--max_workers`: 并行工作进程数
- `--interval`: 数据频率（1d或1min）
- `--rqdata_username`: RQData用户名
- `--rqdata_password`: RQData密码

#### 下载数据参数
- `--start`: 开始日期
- `--end`: 结束日期
- `--max_collector_count`: 最大收集器数量
- `--delay`: 请求延迟（秒）
- `--check_data_length`: 数据长度检查
- `--limit_nums`: 限制下载的股票数量（用于测试）

#### 标准化数据参数
- `--date_field_name`: 日期字段名
- `--symbol_field_name`: 股票代码字段名
- `--end_date`: 结束日期
- `--qlib_data_1d_dir`: 日线数据目录

#### 更新数据参数
- `--qlib_data_1d_dir`: 日线数据目录
- `--end_date`: 结束日期
- `--exists_skip`: 是否跳过已存在的数据
- `--delay`: 请求延迟（秒）

#### 指数数据参数
- `--index_name`: 指数代码
- `--qlib_dir`: QLib数据目录
- `--freq`: 数据频率（day或min）

### 6. 使用示例

#### 示例1：下载并标准化日线数据
```bash
# 1. 下载数据
python scripts/data_collector/rqdata/collector.py download_data \
    --source_dir "./data/source" \
    --normalize_dir "./data/normalize" \
    --interval "1d" \
    --start "2023-01-01" \
    --end "2023-12-31" \
    --rqdata_username "your_username" \
    --rqdata_password "your_password"

# 2. 标准化数据
python scripts/data_collector/rqdata/collector.py normalize_data \
    --source_dir "./data/source" \
    --normalize_dir "./data/normalize" \
    --end_date "2023-12-31" \
    --qlib_data_1d_dir "./data/qlib_data_1d"
```

#### 示例2：增量更新数据
```bash
# 更新最近一个月的数据
python scripts/data_collector/rqdata/collector.py update_data_to_bin \
    --qlib_data_1d_dir "./data/qlib_data_1d" \
    --end_date "$(date +%Y-%m-%d)" \
    --exists_skip True \
    --rqdata_username "your_username" \
    --rqdata_password "your_password"
```

#### 示例3：获取沪深300成分股
```bash
# 获取沪深300成分股数据
python scripts/data_collector/rqdata_index/collector.py get_index_components \
    --index_name "000300.XSHG" \
    --qlib_dir "./data/qlib_data" \
    --freq "day" \
    --rqdata_username "your_username" \
    --rqdata_password "your_password"
```

### 7. 命令行使用提示

1. 路径设置
   - 建议使用绝对路径或相对于项目根目录的路径
   - Windows系统使用反斜杠（\）时需要进行转义或使用正斜杠（/）

2. 参数传递
   - 所有参数都可以通过命令行传递
   - 布尔类型参数（如exists_skip）可以直接使用True/False
   - 日期格式统一使用"YYYY-MM-DD"

3. 日志查看
   - 程序运行日志会输出到控制台
   - 建议使用重定向将日志保存到文件：
     ```bash
     python scripts/data_collector/rqdata/collector.py download_data [参数] > log.txt 2>&1
     ```

4. 错误处理
   - 如果遇到参数错误，程序会显示详细的错误信息
   - 网络错误会自动重试
   - 建议定期检查日志文件

## 常见问题

### 1. 数据获取失败
- 检查RQData账号是否有效
- 确认网络连接是否正常
- 查看日志文件了解具体错误原因

### 2. 数据不完整
- 检查数据时间范围是否正确
- 确认股票代码格式是否正确
- 查看是否有停牌或退市情况

### 3. 性能问题
- 适当调整max_workers参数
- 使用数据缓存功能
- 考虑使用增量更新方式

## 更新日志

### v1.0.0 (2024-03-21)
- 初始版本发布
- 支持日线和分钟线数据获取
- 支持指数成分股数据获取
- 支持数据标准化和预处理
- 支持数据缓存管理 