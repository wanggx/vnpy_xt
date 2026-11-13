# 1.4.6

1. 修复XtGateway断线重连时的传参错误
2. **定时查询异常隔离（大 QMT 管道超时不再拖垮主进程）**：`query_position` /
   `query_account` / `query_order` / `query_trade` 走命名管道 RPC，管道卡住会抛
   `TransportTimeout`。原来这些调用直接在**事件引擎线程**的 `process_timer_event`
   里执行，一抛异常就把事件引擎线程打死，vnpy 主进程随即整体异常。现在：
   - 定时查询改到独立 daemon 线程执行，**事件引擎线程永不阻塞**（实测查询卡 3s 时，
     其他事件仍在 0.25s 内正常分发）；上一轮未返回则跳过本轮，避免任务堆积；
   - `process_timer_event` / `XtTdApi._safe_query` / 行情 `subscribe_quote`、
     `unsubscribe_quote` 全部就地 try/except，异常只记日志（同类错误 30s 内只记一次）；
   - 顺带修掉原实现「`func()` 抛异常就不再把 func 放回队列」的问题——查询队列会被
     抽干，随后 `pop(0)` 直接 `IndexError` 再次打死线程。
3. **交易通道断线自动重连**：识别 `TransportError` / `TransportTimeout`（含
   `ConnectionResetError`/`BrokenPipeError`/`TimeoutError` 等按异常名兜底）后，
   不再一秒一次地硬查，而是：`td_api.connected = False` 暂停定时查询 → 后台线程按
   **指数退避**（2s→4s→8s…上限 60s）重连，每次重连前先 `close()` 释放半死管道 →
   连上后日志提示恢复、定时查询与推送自动继续。`close()` 会 `stop_reconnect()`
   终止重连循环；`send_order` / `cancel_order` 的传输异常同样汇入这条链路；普通业务
   异常（如数据格式错误）不会触发重连。实测（退避压到 0.05s）：第 1、2 次失败、
   第 3 次成功，`connected` 恢复 True，日志依次给出「暂停查询 → 第 n 次失败 → 重连成功」。
4. 新增 `XtDatafeed.get_trading_dates(start, end)`：向大 QMT 取已剔除节假日的
   **交易日历**，供数据完整性检查判断"哪天本来就该有数据"。取不到时返回 `None`
   （调用方退化为只排除周末的粗日历）。该调用在部分终端上较慢（国金 2.1.19.0 实测
   约 2s/次），调用方应按区间缓存、一次检查只取一次。
5. 新增 `XtDatafeed.download_bar_history(req)`：主动让大 QMT 服务端把该区间**原始**
   K 线补进本地缓存（`download_history_data2`），供数据完整性检查「回源复核」用——
   `query_bar_history` 只读 QMT 本地缓存，不先补拉就无法区分「源头没有这段数据」和
   「本地缓存没同步」。只在需要时调用，不影响正常下载流程。
6. 历史 K 线恢复**等比前复权**：`XtDatafeed` 从 `[Mod] 修改历史数据复权方式`（00572ab）起
   一直用 `"front_ratio"`，`501f411 Fix XT datafeed and gateway handling`（2026-07-10）里
   被改成 `"none"`（不复权），之后切大 QMT RPC 时又沿用了下来——于是数据管理（DataManager）
   下载入库的日线/分钟线是不复权价，除权除息日跳空、均线与回测失真。现恢复为
   `DIVIDEND_TYPE = "front_ratio"` 并用于所有 K 线周期；Tick 是逐笔真实成交价，仍走 `none`。
   注意：已入库的历史数据不会自动变化，需在数据管理里用「重下」（`DownloadMode.REPLACE`）
   重新下载；另外前复权价会随每次分红整体位移，而「追加」模式只补缺口、不重下历史，
   长期使用建议分红后对该标的按需「重下」，或改用 `"back_ratio"`（等比后复权，历史段
   不随时间变化）。复权数据由大 QMT 服务端现场计算，需先有原始 K 线（`download_history_data2`
   会自动先下原始数据）；复权请求不走 FormulaServer 快路径，会回退 RPC，速度比不复权慢。

# 1.4.5

1. 更新xtquant版本依赖到250516.1.1

# 1.4.4

1. 修复开盘集合竞价K线的价格没有反映到第一分钟最高价和最低价的问题

# 1.4.3

1. XtGateway限制仅连接VIP服务器地址：115.231.218.73:55310, 115.231.218.79:55310

# 1.4.2

1. 放宽filelock依赖库版本，解决兼容性问题

# 1.4.1

1. 增加行情数据中的市场收盘状态推送

# 1.4.0

1. 委托时仅对期权委托检查开平方向
2. 现货委托自动移除开平方向，保证底层兼容

# 1.3.0

1. vnpy框架4.0版本升级适配

# 1.2.6

1. 增加集合竞价K线volume为0时的过滤

# 1.2.5

1. XtDatafeed限制仅连接VIP服务器地址：115.231.218.73:55310, 115.231.218.79:55310

# 1.2.4

1. 增加实时行情中的涨跌停价字段

# 1.2.3

1. 修复期权合约数据的gateway_name固定为XT的问题

# 1.2.2

1. 升级240920.1.2版本API
2. XtGateway增加仿真交易功能，支持股票和股票期权

# 1.2.1

1. 修复沪深非期权行情订阅的问题

# 1.2.0

1. 增加实时行情接口XtGateway

# 1.1.0

1. 更新底层xtquant到240613.1.1版本
2. 增加期货历史数据集合竞价K线合成支持
3. 开启使用期货真实夜盘时间
4. 基于文件锁实现xtdc单例运行