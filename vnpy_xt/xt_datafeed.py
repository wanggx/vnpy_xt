from datetime import datetime, timedelta, time
from collections.abc import Callable

from pandas import DataFrame
from bigqmt_signal_trader.xtquant_compat import xtdata

from vnpy.trader.constant import Exchange, Interval
from vnpy.trader.object import BarData, TickData, HistoryRequest
from vnpy.trader.datafeed import BaseDatafeed

from .xt_gateway import generate_datetime


INTERVAL_VT2XT: dict[Interval, str] = {
    Interval.MINUTE: "1m",
    Interval.DAILY: "1d",
    Interval.TICK: "tick"
}

INTERVAL_ADJUSTMENT_MAP: dict[Interval, timedelta] = {
    Interval.MINUTE: timedelta(minutes=1),
    Interval.DAILY: timedelta()         # 日线无需进行调整
}

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


class XtDatafeed(BaseDatafeed):
    """大 QMT RPC 历史数据接口（不经 MiniQMT / 迅投研 xtdatacenter）"""

    def __init__(self) -> None:
        """"""
        self.inited: bool = False

    def init(self, output: Callable = print) -> bool:
        """初始化"""
        if self.inited:
            return True

        try:
            xtdata.get_instrument_detail("000001.SZ")
        except Exception as ex:
            output(f"大 QMT 数据服务初始化失败，发生异常：{ex}")
            return False

        self.inited = True
        return True

    def query_bar_history(self, req: HistoryRequest, output: Callable = print) -> list[BarData] | None:
        """查询K线数据"""
        history: list[BarData] = []

        if not self.inited:
            n: bool = self.init(output)
            if not n:
                return history

        df: DataFrame = get_history_df(req, output)
        if df.empty:
            return history

        adjustment: timedelta = INTERVAL_ADJUSTMENT_MAP[req.interval]

        # 遍历解析
        auction_bar: BarData | None = None

        for tp in df.itertuples():
            dt: datetime = generate_datetime(_row_time(tp), millisecond=True)
            dt = dt - adjustment

            # 日线，过滤尚未走完的当日数据
            if req.interval == Interval.DAILY:
                incomplete_bar: bool = (
                    dt.date() == datetime.now().date()
                    and datetime.now().time() < time(hour=15)
                )
                if incomplete_bar:
                    continue
            # 分钟线，过滤盘前集合竞价数据（合并到开盘后第1根K线中）
            else:
                if (
                    req.exchange in (Exchange.SSE, Exchange.SZSE, Exchange.BSE, Exchange.CFFEX)
                    and dt.time() == time(hour=9, minute=29)
                ) or (
                    req.exchange in (Exchange.SHFE, Exchange.INE, Exchange.DCE, Exchange.CZCE, Exchange.GFEX)
                    and dt.time() in (time(hour=8, minute=59), time(hour=20, minute=59))
                ):
                    auction_bar = BarData(
                        symbol=req.symbol,
                        exchange=req.exchange,
                        datetime=dt,
                        open_price=float(tp.open),
                        volume=float(tp.volume),
                        turnover=float(getattr(tp, "amount", 0) or 0),
                        gateway_name="XT"
                    )
                    continue

            # 生成K线对象
            bar: BarData = BarData(
                symbol=req.symbol,
                exchange=req.exchange,
                datetime=dt,
                interval=req.interval,
                volume=float(tp.volume),
                turnover=float(getattr(tp, "amount", 0) or 0),
                open_interest=float(getattr(tp, "openInterest", 0) or 0),
                open_price=float(tp.open),
                high_price=float(tp.high),
                low_price=float(tp.low),
                close_price=float(tp.close),
                gateway_name="XT"
            )

            # 合并集合竞价数据
            if auction_bar and auction_bar.volume:
                bar.open_price = auction_bar.open_price
                bar.high_price = max(bar.high_price, auction_bar.open_price)
                bar.low_price = min(bar.low_price, auction_bar.open_price)
                bar.volume += auction_bar.volume
                bar.turnover += auction_bar.turnover
                auction_bar = None

            history.append(bar)

        return history

    def query_tick_history(self, req: HistoryRequest, output: Callable = print) -> list[TickData] | None:
        """查询Tick数据"""
        history: list[TickData] = []

        if not self.inited:
            n: bool = self.init(output)
            if not n:
                return history

        df: DataFrame = get_history_df(req, output)
        if df.empty:
            return history

        # 遍历解析
        for tp in df.itertuples():
            dt: datetime = generate_datetime(_row_time(tp), millisecond=True)

            bid_price: list[float] = _level_list(getattr(tp, "bidPrice", None))
            ask_price: list[float] = _level_list(getattr(tp, "askPrice", None))
            bid_vol: list[float] = _level_list(getattr(tp, "bidVol", None))
            ask_vol: list[float] = _level_list(getattr(tp, "askVol", None))

            tick: TickData = TickData(
                symbol=req.symbol,
                exchange=req.exchange,
                datetime=dt,
                volume=float(getattr(tp, "volume", 0) or 0),
                turnover=float(getattr(tp, "amount", 0) or 0),
                open_interest=float(getattr(tp, "openInt", 0) or 0),
                open_price=float(getattr(tp, "open", 0) or 0),
                high_price=float(getattr(tp, "high", 0) or 0),
                low_price=float(getattr(tp, "low", 0) or 0),
                last_price=float(getattr(tp, "lastPrice", 0) or 0),
                pre_close=float(getattr(tp, "lastClose", 0) or 0),
                bid_price_1=float(bid_price[0]),
                ask_price_1=float(ask_price[0]),
                bid_volume_1=float(bid_vol[0]),
                ask_volume_1=float(ask_vol[0]),
                gateway_name="XT",
            )

            bid_price_2: float = float(bid_price[1])
            if bid_price_2:
                tick.bid_price_2 = bid_price_2
                tick.bid_price_3 = float(bid_price[2])
                tick.bid_price_4 = float(bid_price[3])
                tick.bid_price_5 = float(bid_price[4])

                tick.ask_price_2 = float(ask_price[1])
                tick.ask_price_3 = float(ask_price[2])
                tick.ask_price_4 = float(ask_price[3])
                tick.ask_price_5 = float(ask_price[4])

                tick.bid_volume_2 = float(bid_vol[1])
                tick.bid_volume_3 = float(bid_vol[2])
                tick.bid_volume_4 = float(bid_vol[3])
                tick.bid_volume_5 = float(bid_vol[4])

                tick.ask_volume_2 = float(ask_vol[1])
                tick.ask_volume_3 = float(ask_vol[2])
                tick.ask_volume_4 = float(ask_vol[3])
                tick.ask_volume_5 = float(ask_vol[4])

            history.append(tick)

        return history


def _row_time(tp: object) -> object:
    for name in ("time", "stime"):
        raw = getattr(tp, name, None)
        if raw is not None:
            return raw
    raw = getattr(tp, "Index", None)
    if isinstance(raw, datetime):
        return raw
    return 0


def _level_list(values: object, size: int = 5) -> list[float]:
    if values is None:
        return [0.0] * size
    data = list(values)[:size]
    while len(data) < size:
        data.append(0.0)
    return [float(item or 0) for item in data]


def get_history_df(req: HistoryRequest, output: Callable = print) -> DataFrame:
    """从大 QMT RPC 取历史 DataFrame（读终端本地/实时库，不经迅投研）。"""
    symbol: str = req.symbol
    exchange: Exchange = req.exchange
    start_dt: datetime = req.start
    end_dt: datetime = req.end
    interval: Interval = req.interval

    if not interval:
        interval = Interval.TICK

    xt_interval: str | None = INTERVAL_VT2XT.get(interval, None)
    if not xt_interval:
        output(f"大 QMT 查询历史数据失败：不支持的时间周期{interval.value}")
        return DataFrame()

    # 为了查询夜盘数据
    end_dt += timedelta(1)

    xt_symbol: str = symbol + "." + EXCHANGE_VT2XT[exchange]
    start: str = start_dt.strftime("%Y%m%d%H%M%S")
    end: str = end_dt.strftime("%Y%m%d%H%M%S")

    if exchange in (Exchange.SSE, Exchange.SZSE) and len(symbol) > 6:
        xt_symbol += "O"

    try:
        data: dict = xtdata.get_market_data_ex(
            field_list=[],
            stock_list=[xt_symbol],
            period=xt_interval,
            start_time=start,
            end_time=end,
            count=-1,
            dividend_type="none",
            fill_data=False,
        ) or {}
    except Exception as ex:
        output(f"大 QMT 查询历史数据失败：{ex}")
        return DataFrame()

    df = None
    if isinstance(data, dict):
        df = data.get(xt_symbol)
        if df is None:
            wanted: str = xt_symbol.upper()
            for key, value in data.items():
                if str(key).upper() == wanted:
                    df = value
                    break
    if df is None or not isinstance(df, DataFrame) or df.empty:
        output(
            f"大 QMT 未返回 {xt_symbol} 的 {xt_interval} 数据。"
            "请在交易端「数据管理」补充对应周期后再查。"
        )
        return DataFrame()

    return df
