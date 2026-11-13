"""验证：指数用 none 复权、股票仍用 front_ratio；空表会换 none 重试。

不连 QMT：把 ``vnpy_xt.xt_datafeed.xtdata`` 换成假对象，记录每次请求的
``dividend_type`` 并按复权方式返回空/有数据。
"""

from datetime import datetime

from pandas import DataFrame

from vnpy.trader.constant import Exchange, Interval
from vnpy.trader.object import HistoryRequest

from vnpy_xt import xt_datafeed
from vnpy_xt.xt_datafeed import XtDatafeed, is_index_symbol


class FakeXtData:
    """只有 dividend_type='none' 才返回数据（模拟指数没有除权数据）。"""

    def __init__(self, only_none: bool = True) -> None:
        self.calls: list[tuple[str, str, str]] = []
        self.heals: list[bool] = []
        self.only_none = only_none

    def get_instrument_detail(self, code: str) -> dict:
        return {"Code": code}

    def get_market_data_ex(
        self,
        field_list=None,
        stock_list=None,
        period="1d",
        start_time="",
        end_time="",
        count=-1,
        dividend_type="none",
        fill_data=False,
        heal=True,
    ) -> dict:
        code: str = list(stock_list or [""])[0]
        self.calls.append((code, period, dividend_type))
        self.heals.append(heal)
        if self.only_none and dividend_type != "none":
            return {code: DataFrame()}
        return {
            code: DataFrame(
                {
                    "time": [int(start_time[:8]) * 1000000],
                    "open": [1.0],
                    "high": [1.0],
                    "low": [1.0],
                    "close": [1.0],
                    "volume": [1.0],
                    "amount": [1.0],
                },
                index=[start_time[:8]],
            )
        }


def make_req(symbol: str, exchange: Exchange, interval: Interval) -> HistoryRequest:
    return HistoryRequest(
        symbol=symbol,
        exchange=exchange,
        start=datetime(2026, 10, 13),
        end=datetime(2026, 11, 13, 23, 59, 59),
        interval=interval,
    )


def test_is_index_symbol_namespaces() -> None:
    """上证 000xxx / 深证 399xxx 是指数；上交所 6 开头、深交所 000xxx 是股票。"""
    assert is_index_symbol("000001", Exchange.SSE)
    assert is_index_symbol("000300", Exchange.SSE)
    assert is_index_symbol("399006", Exchange.SZSE)
    assert not is_index_symbol("600000", Exchange.SSE)
    assert not is_index_symbol("000001", Exchange.SZSE)   # 平安银行
    assert not is_index_symbol("300750", Exchange.SZSE)


def test_index_query_uses_no_adjustment(monkeypatch) -> None:
    """指数请求必须带 dividend_type='none'，否则 QMT 回空表。"""
    fake = FakeXtData()
    monkeypatch.setattr(xt_datafeed, "xtdata", fake)

    datafeed = XtDatafeed()
    bars = datafeed.query_bar_history(
        make_req("000001", Exchange.SSE, Interval.DAILY), lambda msg: None
    )

    assert bars, "指数日线应能取到"
    assert fake.calls == [("000001.SH", "1d", "none")], fake.calls
    # 只读本地库，绝不触发桥侧自愈下载（否则单只指数读空会卡 60s+）
    assert fake.heals == [False], fake.heals


def test_stock_query_keeps_forward_adjustment(monkeypatch) -> None:
    """股票仍按 front_ratio 取（与旧行为一致）。"""
    fake = FakeXtData(only_none=False)
    monkeypatch.setattr(xt_datafeed, "xtdata", fake)

    datafeed = XtDatafeed()
    datafeed.query_bar_history(
        make_req("600000", Exchange.SSE, Interval.DAILY), lambda msg: None
    )

    assert fake.calls == [("600000.SH", "1d", xt_datafeed.DIVIDEND_TYPE)], fake.calls


def test_empty_frame_retries_with_no_adjustment(monkeypatch) -> None:
    """复权口径回空表时，自动换 none 再查一次（非指数标的也能救回来）。"""
    fake = FakeXtData(only_none=True)
    monkeypatch.setattr(xt_datafeed, "xtdata", fake)

    datafeed = XtDatafeed()
    bars = datafeed.query_bar_history(
        make_req("110059", Exchange.SSE, Interval.DAILY), lambda msg: None
    )

    assert bars, "换 none 之后应能取到"
    assert [call[2] for call in fake.calls] == [xt_datafeed.DIVIDEND_TYPE, "none"], (
        fake.calls
    )
