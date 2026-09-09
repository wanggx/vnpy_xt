# VeighNa 大 QMT（国金 ThinkTrader）行情与交易接口

<p align="center">
  <img src ="https://vnpy.oss-cn-shanghai.aliyuncs.com/vnpy-logo.png"/>
</p>

<p align="center">
    <img src ="https://img.shields.io/badge/version-1.4.6-blueviolet.svg"/>
    <img src ="https://img.shields.io/badge/platform-windows-yellow.svg"/>
    <img src ="https://img.shields.io/badge/python-3.10|3.11|3.12|3.13-blue.svg" />
    <img src ="https://img.shields.io/github/license/vnpy/vnpy.svg?color=orange"/>
</p>

## 说明

基于 [xtquant-big-convert](https://github.com/litaolemo/xtquant_big_convert) 对接大 QMT 终端的实时行情、交易与历史数据。
行情与交易均走 Redis/ZMQ RPC，**不使用 MiniQMT、迅投研 Token，也不启动 xtdatacenter**。

支持以下中国金融市场的 K 线和 Tick 数据：

* 股票、基金、债券、ETF期权：
  * SSE：上海证券交易所
  * SZSE：深圳证券交易所
* 期货、期货期权：
  * CFFEX：中国金融期货交易所
  * SHFE：上海期货交易所
  * DCE：大连商品交易所
  * CZCE：郑州商品交易所
  * INE：上海国际能源交易中心
  * GFEX：广州期货交易所


## 安装

安装环境推荐基于4.0.0版本以上的【[**VeighNa Studio**](https://www.vnpy.com/)】。

直接使用pip命令：

```
pip install vnpy_xt
```


或者下载解压后在cmd中运行：

```
pip install .
```

## 使用

本分支只对接大 QMT。请勿安装官方 `xtquant` MiniQMT 客户端包，否则会与 `xtquant-big-convert` 的同名 shim 冲突。

1. 安装本网关及其依赖：`pip install .`（会安装 `xtquant-big-convert[redis]`）。不要把 `xtquant-big-convert` 的源码拷进本仓库。
2. 在大 QMT 中运行 `BIGQMT_REDIS_DRYRUN`（或 ZMQ 等价入口），并保证 VNPY 侧 Redis/账号配置与 QMT 端 `bigqmt_signal_trader_local_config.py` 一致。
3. 在 VeighNa 连接 XT 网关：勾选市场、资金账号（须与 `BIGQMT_ACCOUNT_ID` 一致）。无需填写 QMT 路径、Token。
4. 历史数据服务：全局配置 `datafeed.name = xt` 即可，不再需要迅投研 Token / client 模式。数据来自交易端本地库，缺周期请先在 QMT「数据管理」补充。
