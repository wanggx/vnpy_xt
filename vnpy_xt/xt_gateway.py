from datetime import datetime
from collections.abc import Callable
from threading import Lock, Thread
from typing import Any

from xtquant import xtconstant
from xtquant.xttype import (
    StockAccount,
    XtAsset,
    XtOrder,
    XtPosition,
    XtTrade,
    XtOrderResponse,
    XtCancelOrderResponse,
    XtOrderError,
    XtCancelError
)
from bigqmt_signal_trader.xtquant_compat import (
    BigQmtXtTrader,
    XtQuantTraderCallback,
    configure,
    xtdata,
)

from vnpy.event import EventEngine, EVENT_TIMER, Event
from vnpy.trader.gateway import BaseGateway
from vnpy.trader.object import (
    OrderRequest,
    CancelRequest,
    SubscribeRequest,
    ContractData,
    TickData,
    HistoryRequest,
    OptionType,
    OrderData,
    Status,
    Direction,
    OrderType,
    AccountData,
    PositionData,
    TradeData,
    Offset
)
from vnpy.trader.constant import (
    Exchange,
    Product
)
from vnpy.trader.utility import (
    ZoneInfo,
    round_to
)


# 交易所映射
EXCHANGE_VT2XT: dict[Exchange, str] = {
    Exchange.SSE: "SH",
    Exchange.SZSE: "SZ",
    Exchange.BSE: "BJ",
    Exchange.SHFE: "SF",
    Exchange.CFFEX: "IF",
    Exchange.INE: "INE",
    Exchange.DCE: "DF",
    Exchange.CZCE: "ZF",
    Exchange.GFEX: "GF",
}

EXCHANGE_XT2VT: dict[str, Exchange] = {v: k for k, v in EXCHANGE_VT2XT.items()}
EXCHANGE_XT2VT["SHO"] = Exchange.SSE
EXCHANGE_XT2VT["SZO"] = Exchange.SZSE


# 委托状态映射
STATUS_XT2VT: dict[str, Status] = {
    xtconstant.ORDER_UNREPORTED: Status.SUBMITTING,
    xtconstant.ORDER_WAIT_REPORTING: Status.SUBMITTING,
    xtconstant.ORDER_REPORTED: Status.NOTTRADED,
    xtconstant.ORDER_REPORTED_CANCEL: Status.CANCELLED,
    xtconstant.ORDER_PARTSUCC_CANCEL: Status.CANCELLED,
    xtconstant.ORDER_PART_CANCEL: Status.CANCELLED,
    xtconstant.ORDER_CANCELED: Status.CANCELLED,
    xtconstant.ORDER_PART_SUCC: Status.PARTTRADED,
    xtconstant.ORDER_SUCCEEDED: Status.ALLTRADED,
    xtconstant.ORDER_JUNK: Status.REJECTED
}

# 多空方向映射
DIRECTION_VT2XT: dict[tuple, str] = {
    (Direction.LONG, Offset.NONE): xtconstant.STOCK_BUY,
    (Direction.SHORT, Offset.NONE): xtconstant.STOCK_SELL,
    (Direction.LONG, Offset.OPEN): xtconstant.STOCK_OPTION_BUY_OPEN,
    (Direction.LONG, Offset.CLOSE): xtconstant.STOCK_OPTION_BUY_CLOSE,
    (Direction.SHORT, Offset.OPEN): xtconstant.STOCK_OPTION_SELL_OPEN,
    (Direction.SHORT, Offset.CLOSE): xtconstant.STOCK_OPTION_SELL_CLOSE,
}
DIRECTION_XT2VT: dict[str, tuple] = {v: k for k, v in DIRECTION_VT2XT.items()}

POSDIRECTION_XT2VT: dict[int, Direction] = {
    xtconstant.DIRECTION_FLAG_BUY: Direction.LONG,
    xtconstant.DIRECTION_FLAG_SELL: Direction.SHORT
}

# 委托类型映射
ORDERTYPE_VT2XT: dict[tuple, int] = {
    (Exchange.SSE, OrderType.LIMIT): xtconstant.FIX_PRICE,
    (Exchange.SZSE, OrderType.LIMIT): xtconstant.FIX_PRICE,
    (Exchange.BSE, OrderType.LIMIT): xtconstant.FIX_PRICE,
}
# MiniQMT 限价回报实测为 50；大 QMT / xtquant_compat 透传 passorder 的
# FIX_PRICE=11。缺省或无法识别时 on_stock_order 再按限价兜底，避免回报被丢。
ORDERTYPE_XT2VT: dict[int, OrderType] = {
    50: OrderType.LIMIT,
    xtconstant.FIX_PRICE: OrderType.LIMIT,
}

# 其他常量
CHINA_TZ = ZoneInfo("Asia/Shanghai")       # 中国时区
MIN_SUBSCRIPTION_COUNT: int = 50            # 用户可配置的订阅上限不得低于该值
DEFAULT_SUBSCRIPTION_COUNT: int = 500       # 连接界面默认最大订阅数量
CONTRACT_DETAIL_LOG_STEP: int = 500         # 合约详情进度日志间隔


# 全局缓存字典
symbol_contract_map: dict[str, ContractData] = {}       # 合约数据
symbol_limit_map: dict[str, tuple[float, float]] = {}   # 涨跌停价


class XtGateway(BaseGateway):
    """
    VeighNa 对接大 QMT（国金 ThinkTrader）的行情与交易接口。
    行情与交易均走 xtquant-big-convert RPC，不连接 MiniQMT / xtdatacenter。
    """

    default_name: str = "XT"

    default_setting: dict[str, Any] = {
        "股票市场": ["是", "否"],
        "期货市场": ["是", "否"],
        "期权市场": ["是", "否"],
        "仿真交易": ["是", "否"],
        "账号类型": ["股票", "股票期权", "信用"],
        "资金账号": "",
        "最大订阅数量": DEFAULT_SUBSCRIPTION_COUNT
    }

    exchanges: list[str] = list(EXCHANGE_VT2XT.keys())

    def __init__(self, event_engine: EventEngine, gateway_name: str) -> None:
        """构造函数"""
        super().__init__(event_engine, gateway_name)

        self.md_api: XtMdApi = XtMdApi(self)
        self.td_api: XtTdApi = XtTdApi(self)

        self.trading: bool = False
        self.orders: dict[str, OrderData] = {}
        self.count: int = 0

        self.thread: Thread | None = None

    def connect(self, setting: dict) -> None:
        """连接交易接口"""
        if self.thread:
            return

        self.thread = Thread(target=self._connect, args=(setting,))
        self.thread.start()

    def _connect(self, setting: dict) -> None:
        """连接交易接口"""
        stock_active: bool = setting["股票市场"] == "是"
        futures_active: bool = setting["期货市场"] == "是"
        option_active: bool = setting["期权市场"] == "是"

        try:
            max_subscription_count: int = int(
                setting.get("最大订阅数量", DEFAULT_SUBSCRIPTION_COUNT)
            )
        except (TypeError, ValueError):
            max_subscription_count = DEFAULT_SUBSCRIPTION_COUNT
            self.write_log(
                f"最大订阅数量格式无效，使用默认值{DEFAULT_SUBSCRIPTION_COUNT}"
            )

        if max_subscription_count < MIN_SUBSCRIPTION_COUNT:
            self.write_log(
                f"最大订阅数量不能小于{MIN_SUBSCRIPTION_COUNT}，"
                f"已调整为{MIN_SUBSCRIPTION_COUNT}"
            )
            max_subscription_count = MIN_SUBSCRIPTION_COUNT

        accountid: str = str(setting.get("资金账号") or "").strip()
        if accountid:
            configure(account_id=accountid)
        else:
            self.write_log(
                "未填写资金账号：大 QMT 行情 RPC 无法初始化。"
                "请填写与 QMT 端 BIGQMT_ACCOUNT_ID 相同的资金账号。"
            )

        self.md_api.connect(
            stock_active,
            futures_active,
            option_active,
            max_subscription_count
        )

        self.trading = setting["仿真交易"] == "是"
        if self.trading:
            if not accountid:
                self.write_log("交易未启动：资金账号为空")
                return

            if setting["账号类型"] == "股票":
                account_type: str = "STOCK"
            elif setting["账号类型"] == "信用":
                account_type = "CREDIT"
            else:
                account_type = "STOCK_OPTION"

            self.write_log(
                "交易走大 QMT RPC（资金账号须与 QMT 端 BIGQMT_ACCOUNT_ID 一致）"
            )
            self.td_api.connect(accountid, account_type)
            self.init_query()

    def subscribe(self, req: SubscribeRequest) -> None:
        """订阅行情"""
        self.md_api.subscribe(req)

    def unsubscribe(self, req: SubscribeRequest) -> None:
        """退订行情"""
        self.md_api.unsubscribe(req)

    def send_order(self, req: OrderRequest) -> str:
        """委托下单"""
        if self.trading:
            return self.td_api.send_order(req)
        else:
            self.write_log("委托失败，交易功能未启用")
            return ""

    def cancel_order(self, req: CancelRequest) -> None:
        """委托撤单"""
        if self.trading:
            self.td_api.cancel_order(req)

    def query_account(self) -> None:
        """查询资金"""
        if self.trading:
            self.td_api.query_account()

    def query_position(self) -> None:
        """查询持仓"""
        if self.trading:
            self.td_api.query_position()

    def query_history(self, req: HistoryRequest) -> None:
        """查询历史数据"""
        return None

    def on_order(self, order: OrderData) -> None:
        """推送委托数据"""
        self.orders[order.orderid] = order
        super().on_order(order)

    def get_order(self, orderid: str) -> OrderData:
        """查询委托数据"""
        return self.orders.get(orderid, None)

    def close(self) -> None:
        """关闭接口"""
        self.md_api.close()

        if self.trading:
            self.td_api.close()

    def process_timer_event(self, event: Event) -> None:
        """定时事件处理"""
        self.count += 1
        if self.count < 2:
            return
        self.count = 0

        func = self.query_functions.pop(0)
        func()
        self.query_functions.append(func)

    def init_query(self) -> None:
        """初始化查询任务"""
        self.query_functions: list = [self.query_account, self.query_position]
        self.event_engine.register(EVENT_TIMER, self.process_timer_event)


class XtMdApi:
    """行情API（大 QMT RPC，不经 MiniQMT / xtdatacenter）"""

    def __init__(self, gateway: XtGateway) -> None:
        """构造函数"""
        self.gateway: XtGateway = gateway
        self.gateway_name: str = gateway.gateway_name

        self.inited: bool = False

        # 已成功向XT行情接口订阅的标的集合，元素为XT格式代码（例如600000.SH）。
        # 该集合用于快速判断标的是否已订阅，并兼容原有代码对subscribed的访问。
        self.subscribed: set[str] = set()

        # XT格式代码到订阅号的映射。subscribe_quote返回订阅号，
        # unsubscribe_quote必须使用该订阅号，不能直接使用合约代码退订。
        self.subscription_ids: dict[str, int] = {}

        # 每个标的当前的逻辑订阅者集合。
        # key为XT格式代码，value中的(app_name, subscriber_name)用于区分不同App和策略实例。
        # 多个订阅者共享一个底层XT订阅，只有集合变空时才真正调用XT退订。
        self.subscribers: dict[str, set[tuple[str, str]]] = {}

        # 保护上述订阅状态，避免不同App并发订阅同一标的时重复创建底层订阅号。
        self.subscription_lock: Lock = Lock()

        # 当前Gateway允许同时存在的最大底层单标的订阅数。
        # 默认和最小值均为50，连接时可由用户配置为更大的整数。
        self.max_subscription_count: int = MIN_SUBSCRIPTION_COUNT
        self.last_volume: dict[str, float] = {}

        self.stock_active: bool = False
        self.futures_active: bool = False
        self.option_active: bool = False

    def onMarketData(self, data: dict) -> None:
        """行情推送回调"""
        if not data:
            return

        for xt_symbol, buf in data.items():
            for d in _as_tick_list(buf):
                self._push_tick(xt_symbol, d)

    def _push_tick(self, xt_symbol: str, d: dict) -> None:
        """将单笔 tick 转成 TickData 推送。"""
        if "." not in str(xt_symbol):
            return

        symbol, xt_exchange = xt_symbol.split(".", 1)
        exchange = EXCHANGE_XT2VT.get(xt_exchange)
        if exchange is None:
            return

        tick_time = d.get("time") or d.get("timetag") or 0
        try:
            tick_time = int(tick_time)
        except (TypeError, ValueError):
            tick_time = 0

        tick: TickData = TickData(
            symbol=symbol,
            exchange=exchange,
            datetime=generate_datetime(tick_time) if tick_time else datetime.now(CHINA_TZ),
            volume=d.get("volume") or 0,
            turnover=d.get("amount") or 0,
            open_interest=d.get("openInt") or d.get("openInterest") or 0,
            gateway_name=self.gateway_name
        )

        contract = symbol_contract_map.get(tick.vt_symbol)
        if not contract:
            return
        tick.name = contract.name

        previous_volume: float | None = self.last_volume.get(tick.vt_symbol)
        self.last_volume[tick.vt_symbol] = tick.volume
        if previous_volume is not None:
            tick.last_volume = max(tick.volume - previous_volume, 0)

        bp_data: list = _pad_level(d.get("bidPrice") or d.get("bidPriceList"))
        ap_data: list = _pad_level(d.get("askPrice") or d.get("askPriceList"))
        bv_data: list = _pad_level(d.get("bidVol") or d.get("bidVolume") or d.get("bidVolList"))
        av_data: list = _pad_level(d.get("askVol") or d.get("askVolume") or d.get("askVolList"))

        tick.bid_price_1 = round_to(bp_data[0], contract.pricetick)
        tick.bid_price_2 = round_to(bp_data[1], contract.pricetick)
        tick.bid_price_3 = round_to(bp_data[2], contract.pricetick)
        tick.bid_price_4 = round_to(bp_data[3], contract.pricetick)
        tick.bid_price_5 = round_to(bp_data[4], contract.pricetick)

        tick.ask_price_1 = round_to(ap_data[0], contract.pricetick)
        tick.ask_price_2 = round_to(ap_data[1], contract.pricetick)
        tick.ask_price_3 = round_to(ap_data[2], contract.pricetick)
        tick.ask_price_4 = round_to(ap_data[3], contract.pricetick)
        tick.ask_price_5 = round_to(ap_data[4], contract.pricetick)

        tick.bid_volume_1 = bv_data[0]
        tick.bid_volume_2 = bv_data[1]
        tick.bid_volume_3 = bv_data[2]
        tick.bid_volume_4 = bv_data[3]
        tick.bid_volume_5 = bv_data[4]

        tick.ask_volume_1 = av_data[0]
        tick.ask_volume_2 = av_data[1]
        tick.ask_volume_3 = av_data[2]
        tick.ask_volume_4 = av_data[3]
        tick.ask_volume_5 = av_data[4]

        last_price = d.get("lastPrice") or d.get("last") or 0
        tick.last_price = round_to(last_price, contract.pricetick)
        tick.open_price = round_to(d.get("open") or 0, contract.pricetick)
        tick.high_price = round_to(d.get("high") or 0, contract.pricetick)
        tick.low_price = round_to(d.get("low") or 0, contract.pricetick)
        tick.pre_close = round_to(d.get("lastClose") or d.get("preClose") or 0, contract.pricetick)

        if tick.vt_symbol in symbol_limit_map:
            tick.limit_up, tick.limit_down = symbol_limit_map[tick.vt_symbol]

        settlement = d.get("settlementPrice") or d.get("settlement") or 0
        tick.extra = {
            "raw": d,
            "market_closed": False,
        }

        if contract.product not in {Product.FUTURES, Product.OPTION}:
            tick.extra["market_closed"] = d.get("openInt") == 15
        elif settlement > 0:
            tick.extra["market_closed"] = True

        self.gateway.on_tick(tick)

    def connect(
        self,
        stock_active: bool,
        futures_active: bool,
        option_active: bool,
        max_subscription_count: int = MIN_SUBSCRIPTION_COUNT
    ) -> None:
        """连接大 QMT 行情 RPC（不启动 xtdatacenter）"""
        self.gateway.write_log("开始连接大 QMT 行情 RPC，请稍等")

        self.stock_active = stock_active
        self.futures_active = futures_active
        self.option_active = option_active
        self.max_subscription_count = max_subscription_count

        if self.inited:
            self.gateway.write_log("行情接口已经初始化，请勿重复操作")
            return

        try:
            xtdata.get_instrument_detail("000001.SZ")
        except Exception as ex:
            self.gateway.write_log(f"大 QMT 行情初始化失败，发生异常：{ex}")
            return

        self.inited = True

        self.gateway.write_log("行情接口连接成功")
        self.gateway.write_log(
            f"行情最大订阅数量：{self.max_subscription_count}"
        )

        self.query_contracts()

    def query_contracts(self) -> None:
        """查询合约信息"""
        started: datetime = datetime.now()
        self.gateway.write_log(
            "开始查询合约信息："
            f"股票={'是' if self.stock_active else '否'} "
            f"期货={'是' if self.futures_active else '否'} "
            f"期权={'是' if self.option_active else '否'} "
            "（RPC 逐只拉详情，股票全市场可能需要一两分钟）"
        )

        if self.stock_active:
            self.query_stock_contracts()

        if self.futures_active:
            self.query_future_contracts()

        if self.option_active:
            self.query_option_contracts()

        elapsed: float = (datetime.now() - started).total_seconds()
        self.gateway.write_log(
            f"合约信息查询成功，合计 {len(symbol_contract_map)} 只，耗时 {elapsed:.1f} 秒"
        )

    def _collect_sector_codes(self, markets: list) -> list[str]:
        xt_symbols: list[str] = []
        for name in markets:
            xt_symbols.extend(xtdata.get_stock_list_in_sector(name) or [])
        return list(dict.fromkeys(xt_symbols))

    def _log_detail_progress(self, label: str, index: int, total: int) -> None:
        if index % CONTRACT_DETAIL_LOG_STEP == 0:
            self.gateway.write_log(f"{label}合约详情进度 {index}/{total}")

    def query_stock_contracts(self) -> None:
        """查询股票合约信息（仅沪深京A股，不含ETF和指数）"""
        xt_symbols: list[str] = self._collect_sector_codes([
            "沪深A股",
            "京市A股"
        ])
        total: int = len(xt_symbols)

        for index, xt_symbol in enumerate(xt_symbols, start=1):
            try:
                symbol, xt_exchange = xt_symbol.split(".")
                if xt_exchange not in ("SH", "SZ", "BJ"):
                    continue

                data: dict = xtdata.get_instrument_detail(xt_symbol)
                if data is None:
                    continue

                contract: ContractData = ContractData(
                    symbol=symbol,
                    exchange=EXCHANGE_XT2VT[xt_exchange],
                    name=data["InstrumentName"],
                    product=Product.EQUITY,
                    size=data["VolumeMultiple"],
                    pricetick=data["PriceTick"],
                    history_data=False,
                    gateway_name=self.gateway_name
                )

                symbol_contract_map[contract.vt_symbol] = contract
                symbol_limit_map[contract.vt_symbol] = (data["UpStopPrice"], data["DownStopPrice"])

                self.gateway.on_contract(contract)
            finally:
                self._log_detail_progress("股票", index, total)

    def query_future_contracts(self) -> None:
        """查询期货合约信息"""
        xt_symbols: list[str] = self._collect_sector_codes([
            "中金所期货",
            "上期所期货",
            "能源中心期货",
            "大商所期货",
            "郑商所期货",
            "广期所期货"
        ])
        total: int = len(xt_symbols)

        for index, xt_symbol in enumerate(xt_symbols, start=1):
            try:
                # 筛选需要的合约
                product = None
                symbol, xt_exchange = xt_symbol.split(".")

                if xt_exchange == "ZF" and len(symbol) > 6 and "&" not in symbol:
                    product = Product.OPTION
                elif xt_exchange in ("IF", "GF") and "-" in symbol:
                    product = Product.OPTION
                elif xt_exchange in ("DF", "INE", "SF") and ("C" in symbol or "P" in symbol) and "SP" not in symbol:
                    product = Product.OPTION
                else:
                    product = Product.FUTURES

                # 生成并推送合约信息
                if product == Product.OPTION:
                    data: dict = xtdata.get_instrument_detail(xt_symbol, True)
                else:
                    data = xtdata.get_instrument_detail(xt_symbol)

                if not data["ExpireDate"]:
                    if "00" not in symbol:
                        continue

                contract: ContractData = ContractData(
                    symbol=symbol,
                    exchange=EXCHANGE_XT2VT[xt_exchange],
                    name=data["InstrumentName"],
                    product=product,
                    size=data["VolumeMultiple"],
                    pricetick=data["PriceTick"],
                    history_data=False,
                    gateway_name=self.gateway_name
                )

                symbol_contract_map[contract.vt_symbol] = contract
                symbol_limit_map[contract.vt_symbol] = (data["UpStopPrice"], data["DownStopPrice"])

                self.gateway.on_contract(contract)
            finally:
                self._log_detail_progress("期货", index, total)

    def query_option_contracts(self) -> None:
        """查询期权合约信息"""
        xt_symbols: list[str] = self._collect_sector_codes([
            "上证期权",
            "深证期权",
            "中金所期权",
            "上期所期权",
            "能源中心期权",
            "大商所期权",
            "郑商所期权",
            "广期所期权"
        ])
        total: int = len(xt_symbols)

        for index, xt_symbol in enumerate(xt_symbols, start=1):
            try:
                _, xt_exchange = xt_symbol.split(".")

                if xt_exchange in {"SHO", "SZO"}:
                    contract = process_etf_option(xtdata.get_instrument_detail, xt_symbol, self.gateway_name)
                else:
                    contract = process_futures_option(xtdata.get_instrument_detail, xt_symbol, self.gateway_name)

                if contract:
                    symbol_contract_map[contract.vt_symbol] = contract
                    self.gateway.on_contract(contract)
            finally:
                self._log_detail_progress("期权", index, total)

    def subscribe(self, req: SubscribeRequest) -> None:
        """按订阅者汇总需求，并在必要时创建XT底层行情订阅。"""
        # 只有已经查询到合约信息的标的才能订阅。
        if req.vt_symbol not in symbol_contract_map:
            return

        # 将VeighNa合约代码转换为XT格式代码，ETF期权需要使用SHO/SZO后缀。
        xt_exchange: str = EXCHANGE_VT2XT[req.exchange]
        if xt_exchange in {"SH", "SZ"} and len(req.symbol) > 6:
            xt_exchange += "O"

        xt_symbol: str = req.symbol + "." + xt_exchange
        subscriber: tuple[str, str] = (req.app_name, req.subscriber_name)

        with self.subscription_lock:
            # 底层订阅已存在时，只登记新的逻辑订阅者，不重复调用subscribe_quote。
            if xt_symbol in self.subscription_ids:
                self.subscribers.setdefault(xt_symbol, set()).add(subscriber)
                self.gateway.write_log(
                    f"行情订阅完成，当前订阅标的数量：{len(self.subscription_ids)}"
                )
                return

            # 限制的是XT底层单标的订阅数量，而不是共享该标的的策略数量。
            # 已订阅标的达到上限后拒绝新标的，不保存订阅者，调用方之后可以重试。
            if len(self.subscription_ids) >= self.max_subscription_count:
                self.gateway.write_log(
                    f"行情订阅失败，已达到最大订阅数量"
                    f"{self.max_subscription_count}：{xt_symbol}"
                )
                return

            # 先登记逻辑订阅者，再向XT创建唯一的底层订阅。
            subscribers: set[tuple[str, str]] = self.subscribers.setdefault(xt_symbol, set())
            subscribers.add(subscriber)

            subscription_id: int = xtdata.subscribe_quote(
                stock_code=xt_symbol,
                period="tick",
                callback=self.onMarketData
            )

            # XT返回值大于0才表示订阅成功。失败时回滚订阅者记录，避免留下脏状态。
            if subscription_id <= 0:
                subscribers.remove(subscriber)
                if not subscribers:
                    self.subscribers.pop(xt_symbol)

                self.gateway.write_log(f"行情订阅失败：{xt_symbol}")
                return

            self.subscription_ids[xt_symbol] = subscription_id
            self.subscribed.add(xt_symbol)
            self.gateway.write_log(
                f"行情订阅完成，当前订阅标的数量：{len(self.subscription_ids)}"
            )

    def unsubscribe(self, req: SubscribeRequest) -> None:
        """移除逻辑订阅者，并在最后一个订阅者退出时退订XT行情。"""
        # 使用与subscribe完全相同的规则生成XT格式代码。
        xt_exchange: str = EXCHANGE_VT2XT[req.exchange]
        if xt_exchange in {"SH", "SZ"} and len(req.symbol) > 6:
            xt_exchange += "O"

        xt_symbol: str = req.symbol + "." + xt_exchange
        subscriber: tuple[str, str] = (req.app_name, req.subscriber_name)

        with self.subscription_lock:
            # 没有该标的的订阅记录时无需处理。
            subscribers: set[tuple[str, str]] | None = self.subscribers.get(xt_symbol)
            if subscribers is None:
                return

            # 调用者并未订阅该标的时不能影响其他订阅者。
            if subscriber not in subscribers:
                return

            # 只移除当前App/策略实例的需求；仍有其他订阅者时保留底层XT订阅。
            subscribers.remove(subscriber)
            if subscribers:
                self.gateway.write_log(
                    f"行情退订完成，当前订阅标的数量：{len(self.subscription_ids)}"
                )
                return

            # 最后一个订阅者已经退出，清理逻辑状态并使用保存的订阅号退订XT行情。
            self.subscribers.pop(xt_symbol)
            subscription_id: int | None = self.subscription_ids.pop(xt_symbol, None)
            if subscription_id is None:
                self.gateway.write_log(
                    f"行情退订完成，当前订阅标的数量：{len(self.subscription_ids)}"
                )
                return

            xtdata.unsubscribe_quote(subscription_id)
            self.subscribed.discard(xt_symbol)
            self.last_volume.pop(req.vt_symbol, None)
            self.gateway.write_log(
                f"行情退订完成，当前订阅标的数量：{len(self.subscription_ids)}"
            )

    def close(self) -> None:
        """关闭连接"""
        with self.subscription_lock:
            for subscription_id in self.subscription_ids.values():
                xtdata.unsubscribe_quote(subscription_id)

            self.subscribed.clear()
            self.subscription_ids.clear()
            self.subscribers.clear()
            self.last_volume.clear()


class XtTdApi(XtQuantTraderCallback):
    """交易API"""

    def __init__(self, gateway: XtGateway):
        """构造函数"""
        super().__init__()

        self.gateway: XtGateway = gateway
        self.gateway_name: str = gateway.gateway_name

        self.inited: bool = False
        self.connected: bool = False

        self.account_id: str = ""
        self.account_type: str = ""

        self.order_count: int = 0

        self.active_localid_sysid_map: dict[str, str] = {}

        self.xt_client: BigQmtXtTrader = None
        self.xt_account: StockAccount = None

    def on_connected(self) -> None:
        """
        连接成功推送
        """
        self.gateway.write_log("交易接口连接成功")

    def on_disconnected(self) -> None:
        """连接断开"""
        self.gateway.write_log("交易接口连接断开，请检查与客户端的连接状态")
        self.connected = False

        # 尝试重连，重连需要更换session_id
        session: int = int(float(datetime.now().strftime("%H%M%S.%f")) * 1000)
        connect_result: int = self.connect(self.account_id, self.account_type, session)

        if connect_result:
            self.gateway.write_log("交易接口重连失败")
        else:
            self.gateway.write_log("交易接口重连成功")

    def on_stock_trade(self, xt_trade: XtTrade) -> None:
        """成交变动推送"""
        if not xt_trade.order_remark:
            return

        symbol, xt_exchange = xt_trade.stock_code.split(".")

        direction, offset = DIRECTION_XT2VT.get(xt_trade.order_type, (None, None))
        if direction is None:
            return

        trade: TradeData = TradeData(
            symbol=symbol,
            exchange=EXCHANGE_XT2VT[xt_exchange],
            orderid=xt_trade.order_remark,
            tradeid=xt_trade.traded_id,
            direction=direction,
            offset=offset,
            price=xt_trade.traded_price,
            volume=xt_trade.traded_volume,
            datetime=generate_datetime(xt_trade.traded_time, False),
            gateway_name=self.gateway_name
        )

        contract: ContractData = symbol_contract_map.get(trade.vt_symbol, None)
        if contract:
            trade.price = round_to(trade.price, contract.pricetick)

        self.gateway.on_trade(trade)

    def on_stock_order(self, xt_order: XtOrder) -> None:
        """委托回报推送"""
        # 过滤非VeighNa Trader发出的委托
        if not xt_order.order_remark:
            return

        # 过滤不支持的委托类型。大 QMT 回报常见 FIX_PRICE=11，或缺少 price_type。
        type: OrderType = ORDERTYPE_XT2VT.get(xt_order.price_type, None)
        if type is None and xt_order.price_type in (None, ""):
            type = OrderType.LIMIT
        if not type:
            return

        direction, offset = DIRECTION_XT2VT.get(xt_order.order_type, (None, None))
        if direction is None:
            return

        symbol, xt_exchange = xt_order.stock_code.split(".")

        order: OrderData = OrderData(
            symbol=symbol,
            exchange=EXCHANGE_XT2VT[xt_exchange],
            orderid=xt_order.order_remark,
            direction=direction,
            offset=offset,
            type=type,                  # 目前测出来与文档不同，限价返回50，市价返回88
            price=xt_order.price,
            volume=xt_order.order_volume,
            traded=xt_order.traded_volume,
            status=STATUS_XT2VT.get(xt_order.order_status, Status.SUBMITTING),
            datetime=generate_datetime(xt_order.order_time, False),
            gateway_name=self.gateway_name
        )

        if order.is_active():
            self.active_localid_sysid_map[xt_order.order_remark] = xt_order.order_sysid
        else:
            self.active_localid_sysid_map.pop(xt_order.order_remark, None)

        contract: ContractData = symbol_contract_map.get(order.vt_symbol, None)
        if contract:
            order.price = round_to(order.price, contract.pricetick)

        self.gateway.on_order(order)

    def on_query_order_async(self, xt_orders: list[XtOrder]) -> None:
        """委托信息异步查询回报"""
        if not xt_orders:
            return

        for data in xt_orders:
            self.on_stock_order(data)

        self.gateway.write_log("委托信息查询成功")

    def on_query_asset_async(self, xt_asset: XtAsset) -> None:
        """资金信息异步查询回报"""
        if not xt_asset:
            return

        account: AccountData = AccountData(
            accountid=xt_asset.account_id,
            balance=xt_asset.total_asset,
            frozen=xt_asset.frozen_cash,
            gateway_name=self.gateway_name
        )
        account.available = xt_asset.cash

        self.gateway.on_account(account)

    def on_query_trades_async(self, xt_trades: list[XtTrade]) -> None:
        """成交信息异步查询回报"""
        if not xt_trades:
            return

        for xt_trade in xt_trades:
            self.on_stock_trade(xt_trade)

        self.gateway.write_log("成交信息查询成功")

    def on_query_positions_async(self, xt_positions: list[XtPosition]) -> None:
        """持仓信息异步查询回报"""
        if not xt_positions:
            return

        for xt_position in xt_positions:
            if self.account_type == "STOCK":
                direction: Direction = Direction.NET
            else:
                direction = POSDIRECTION_XT2VT.get(xt_position.direction, "")

            if not direction:
                continue

            symbol, xt_exchange = xt_position.stock_code.split(".")

            position: PositionData = PositionData(
                symbol=symbol,
                exchange=EXCHANGE_XT2VT[xt_exchange],
                direction=direction,
                volume=xt_position.volume,
                yd_volume=xt_position.can_use_volume,
                frozen=xt_position.volume - xt_position.can_use_volume,
                price=xt_position.open_price,
                gateway_name=self.gateway_name
            )

            self.gateway.on_position(position)

    def on_order_error(self, xt_error: XtOrderError) -> None:
        """委托失败推送"""
        order: OrderData = self.gateway.get_order(xt_error.order_remark)
        if order:
            order.status = Status.REJECTED
            self.gateway.on_order(order)

        self.gateway.write_log(f"交易委托失败, 错误代码{xt_error.error_id}, 错误信息{xt_error.error_msg}")

    def on_cancel_error(self, xt_error: XtCancelError) -> None:
        """撤单失败推送"""
        self.gateway.write_log(f"交易撤单失败, 错误代码{xt_error.error_id}, 错误信息{xt_error.error_msg}")

    def on_order_stock_async_response(self, response: XtOrderResponse) -> None:
        """异步下单回报推送"""
        if response.error_msg:
            self.gateway.write_log(f"委托请求提交失败：{response.error_msg}，本地委托号{response.order_remark}")
        else:
            self.gateway.write_log(f"委托请求提交成功，本地委托号{response.order_remark}")

    def on_cancel_order_stock_async_response(self, response: XtCancelOrderResponse) -> None:
        """异步撤单回报推送"""
        if response.error_msg:
            self.gateway.write_log(f"撤单请求提交失败：{response.error_msg}，系统委托号{response.order_sysid}")
        else:
            self.gateway.write_log(f"撤单请求提交成功，系统委托号{response.order_sysid}")

    def connect(
        self,
        accountid: str,
        account_type: str,
        session: int = 0
    ) -> int:
        """发起连接"""
        self.inited = True
        self.account_id = accountid
        self.account_type = account_type

        if not session:
            session = int(float(datetime.now().strftime("%H%M%S.%f")) * 1000)

        self.xt_client = BigQmtXtTrader(
            session_id=session,
            account_id=self.account_id,
        )

        self.xt_account = StockAccount(self.account_id, account_type=self.account_type)

        # 注册回调接口
        self.xt_client.register_callback(self)

        # 启动交易线程
        self.xt_client.start()

        # 建立交易连接，返回0表示连接成功
        connect_result: int = self.xt_client.connect()
        if connect_result:
            self.gateway.write_log("交易接口连接失败" + str(connect_result))
            return connect_result

        self.connected = True
        self.gateway.write_log("交易接口连接成功")

        # 订阅交易回调推送
        subscribe_result: int = self.xt_client.subscribe(self.xt_account)
        if subscribe_result:
            self.gateway.write_log("交易推送订阅失败")
            return -1

        self.gateway.write_log("交易推送订阅成功")

        # 初始化数据查询
        self.query_account()
        self.query_position()
        self.query_order()
        self.query_trade()

        return connect_result

    def new_orderid(self) -> str:
        """生成本地委托号"""
        prefix: str = datetime.now().strftime("1%m%d%H%M%S")

        self.order_count += 1
        suffix: str = str(self.order_count).rjust(6, "0")

        orderid: str = prefix + suffix
        return orderid

    def send_order(self, req: OrderRequest) -> str:
        """委托下单"""
        if not self.connected:
            self.gateway.write_log("委托失败，交易接口尚未连接")
            return ""

        contract: ContractData = symbol_contract_map.get(req.vt_symbol, None)
        if not contract:
            self.gateway.write_log(f"找不到该合约{req.vt_symbol}")
            return ""

        if contract.exchange not in {Exchange.SSE, Exchange.SZSE, Exchange.BSE}:
            self.gateway.write_log(f"不支持的合约{req.vt_symbol}")
            return ""

        if req.type not in {OrderType.LIMIT}:
            self.gateway.write_log(f"不支持的委托类型: {req.type.value}")
            return ""

        if req.offset == Offset.NONE and contract.product == Product.OPTION:
            self.gateway.write_log("委托失败，期权交易需要选择开平方向")
            return ""

        stock_code: str = req.symbol + "." + EXCHANGE_VT2XT[req.exchange]
        if self.account_type == "STOCK_OPTION":
            stock_code += "O"

        # 现货委托不考虑开平
        if contract.product == Product.OPTION:
            xt_direction: tuple = (req.direction, req.offset)
        else:
            xt_direction = (req.direction, Offset.NONE)

        orderid: str = self.new_orderid()

        try:
            self.xt_client.order_stock_async(
                account=self.xt_account,
                stock_code=stock_code,
                order_type=DIRECTION_VT2XT[xt_direction],
                order_volume=int(req.volume),
                price_type=ORDERTYPE_VT2XT[(req.exchange, req.type)],
                price=req.price,
                strategy_name=req.reference,
                order_remark=orderid
            )
        except Exception as ex:
            self.gateway.write_log(f"委托请求发送异常：{ex}")
            return ""

        order: OrderData = req.create_order_data(orderid, self.gateway_name)
        self.gateway.on_order(order)

        vt_orderid: str = order.vt_orderid
        self.gateway.write_log(
            f"委托请求已发送：{stock_code} {req.direction.value} "
            f"{int(req.volume)}@{req.price}，本地委托号{orderid}"
        )

        return vt_orderid

    def cancel_order(self, req: CancelRequest) -> None:
        """委托撤单"""
        sysid: str | None = self.active_localid_sysid_map.get(req.orderid, None)
        if not sysid:
            self.gateway.write_log("撤单失败，找不到委托号")
            return

        if req.exchange == Exchange.SSE:
            market: int = 0
        else:
            market = 1

        self.xt_client.cancel_order_stock_sysid_async(self.xt_account, market, sysid)

    def query_position(self) -> None:
        """查询持仓"""
        if self.connected:
            self.xt_client.query_stock_positions_async(
                self.xt_account, callback=self.on_query_positions_async
            )

    def query_account(self) -> None:
        """查询账户资金"""
        if self.connected:
            self.xt_client.query_stock_asset_async(
                self.xt_account, callback=self.on_query_asset_async
            )

    def query_order(self) -> None:
        """查询委托信息"""
        if self.connected:
            # 必须用 callback=：大 QMT 兼容层第二参是 cancelable_only，
            # 位置传回调会变成 True 且永远不触发 on_query_order_async。
            self.xt_client.query_stock_orders_async(
                self.xt_account, callback=self.on_query_order_async
            )

    def query_trade(self) -> None:
        """查询成交信息"""
        if self.connected:
            self.xt_client.query_stock_trades_async(
                self.xt_account, callback=self.on_query_trades_async
            )

    def close(self) -> None:
        """关闭连接"""
        if self.inited:
            self.xt_client.stop()


def _as_tick_list(buf: Any) -> list[dict]:
    """MiniQMT subscribe_quote 是 {code: [tick, ...]}；大 QMT 全推是 {code: tick_dict}。"""
    if buf is None:
        return []
    if isinstance(buf, list):
        return [item for item in buf if isinstance(item, dict)]
    if isinstance(buf, dict):
        return [buf]
    return []


def _pad_level(values: Any, size: int = 5) -> list[float]:
    """五档不足时补 0，避免下标越界。"""
    if not values:
        return [0.0] * size
    data = list(values)[:size]
    while len(data) < size:
        data.append(0.0)
    return [float(item or 0) for item in data]


def generate_datetime(timestamp: int | float | str | datetime, millisecond: bool = True) -> datetime:
    """生成本地时间。识别 datetime、YYYYMMDD[HHMMSS]、秒 / 毫秒 Unix 时间戳。"""
    if isinstance(timestamp, datetime):
        dt: datetime = timestamp
        if dt.tzinfo is None:
            return dt.replace(tzinfo=CHINA_TZ)
        return dt.astimezone(CHINA_TZ)

    if isinstance(timestamp, str):
        text: str = timestamp.strip()
        for fmt in ("%Y%m%d%H%M%S", "%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M:%S.%f", "%Y%m%d"):
            try:
                return datetime.strptime(text, fmt).replace(tzinfo=CHINA_TZ)
            except ValueError:
                continue
        try:
            timestamp = int(float(text))
        except (TypeError, ValueError):
            return datetime.now(CHINA_TZ)

    ts = int(timestamp or 0)
    if ts <= 0:
        return datetime.now(CHINA_TZ)
    # 大 QMT K 线常见 YYYYMMDD / YYYYMMDDHHMMSS，须先于 Unix 毫秒判断
    if 19_000_101 <= ts <= 20_991_231:
        return datetime.strptime(str(ts), "%Y%m%d").replace(tzinfo=CHINA_TZ)
    if 19_000_101_000_000 <= ts <= 20_991_231_235_959:
        return datetime.strptime(str(ts), "%Y%m%d%H%M%S").replace(tzinfo=CHINA_TZ)
    # 秒级约 10 位，毫秒级 13 位
    if millisecond and ts > 10_000_000_000:
        dt = datetime.fromtimestamp(ts / 1000)
    else:
        dt = datetime.fromtimestamp(ts)
    return dt.replace(tzinfo=CHINA_TZ)


def process_etf_option(get_instrument_detail: Callable, xt_symbol: str, gateway_name: str) -> ContractData | None:
    """处理ETF期权"""
    # 拆分XT代码
    symbol, xt_exchange = xt_symbol.split(".")

    # 筛选期权合约合约（ETF期权代码为8位）
    if len(symbol) != 8:
        return None

    # 查询转换数据
    data: dict = get_instrument_detail(xt_symbol, True)

    name: str = data["InstrumentName"]
    if "购" in name:
        option_type = OptionType.CALL
    elif "沽" in name:
        option_type = OptionType.PUT
    else:
        return None

    if "A" in name:
        option_index = str(data["OptExercisePrice"]) + "-A"
    else:
        option_index = str(data["OptExercisePrice"]) + "-M"

    contract: ContractData = ContractData(
        symbol=data["InstrumentID"],
        exchange=EXCHANGE_XT2VT[xt_exchange],
        name=data["InstrumentName"],
        product=Product.OPTION,
        size=data["VolumeMultiple"],
        pricetick=data["PriceTick"],
        min_volume=data["MinLimitOrderVolume"],
        option_strike=data["OptExercisePrice"],
        option_listed=datetime.strptime(data["OpenDate"], "%Y%m%d"),
        option_expiry=datetime.strptime(data["ExpireDate"], "%Y%m%d"),
        option_portfolio=data["OptUndlCode"] + "_O",
        option_index=option_index,
        option_type=option_type,
        option_underlying=data["OptUndlCode"] + "-" + str(data["ExpireDate"])[:6],
        gateway_name=gateway_name
    )

    symbol_limit_map[contract.vt_symbol] = (data["UpStopPrice"], data["DownStopPrice"])

    return contract


def process_futures_option(get_instrument_detail: Callable, xt_symbol: str, gateway_name: str) -> ContractData | None:
    """处理期货期权"""
    # 筛选期权合约
    data: dict = get_instrument_detail(xt_symbol, True)

    option_strike: float = data["OptExercisePrice"]
    if not option_strike:
        return None

    # 拆分XT代码
    symbol, xt_exchange = xt_symbol.split(".")

    # 移除产品前缀
    for _ix, w in enumerate(symbol):
        if w.isdigit():
            break

    suffix: str = symbol[_ix:]

    # 过滤非期权合约
    if "(" in symbol or " " in symbol:
        return None

    # 判断期权类型
    if "C" in suffix:
        option_type = OptionType.CALL
    elif "P" in suffix:
        option_type = OptionType.PUT
    else:
        return None

    # 获取期权标的
    if "-" in symbol:
        option_underlying: str = symbol.split("-")[0]
    else:
        option_underlying = data["OptUndlCode"]

    # 转换数据
    contract: ContractData = ContractData(
        symbol=data["InstrumentID"],
        exchange=EXCHANGE_XT2VT[xt_exchange],
        name=data["InstrumentName"],
        product=Product.OPTION,
        size=data["VolumeMultiple"],
        pricetick=data["PriceTick"],
        min_volume=data["MinLimitOrderVolume"],
        option_strike=data["OptExercisePrice"],
        option_listed=datetime.strptime(data["OpenDate"], "%Y%m%d"),
        option_expiry=datetime.strptime(data["ExpireDate"], "%Y%m%d"),
        option_index=str(data["OptExercisePrice"]),
        option_type=option_type,
        option_underlying=option_underlying,
        gateway_name=gateway_name
    )

    if contract.exchange == Exchange.CZCE:
        contract.option_portfolio = data["ProductID"][:-1]
    else:
        contract.option_portfolio = data["ProductID"]

    symbol_limit_map[contract.vt_symbol] = (data["UpStopPrice"], data["DownStopPrice"])

    return contract
