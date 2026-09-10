from unittest.mock import Mock

from pytest import MonkeyPatch

from vnpy.event import EventEngine
from vnpy.trader.constant import Exchange
from vnpy.trader.event import EVENT_TICK_UNSUBSCRIBE
from vnpy.trader.object import SubscribeRequest

from vnpy_xt.xt_gateway import (
    DEFAULT_SUBSCRIPTION_COUNT,
    MIN_SUBSCRIPTION_COUNT,
    XtGateway,
    XtMdApi,
    symbol_contract_map,
    xtdata,
)


def create_request(app_name: str, subscriber_name: str) -> SubscribeRequest:
    return SubscribeRequest(
        symbol="600000",
        exchange=Exchange.SSE,
        app_name=app_name,
        subscriber_name=subscriber_name
    )


def test_unsubscribe_after_last_subscriber(monkeypatch: MonkeyPatch) -> None:
    gateway: Mock = Mock()
    gateway.gateway_name = "XT"
    md_api: XtMdApi = XtMdApi(gateway)

    subscribe_quote: Mock = Mock(return_value=101)
    unsubscribe_quote: Mock = Mock()
    monkeypatch.setattr(xtdata, "subscribe_quote", subscribe_quote)
    monkeypatch.setattr(xtdata, "unsubscribe_quote", unsubscribe_quote)

    cta_req: SubscribeRequest = create_request("CtaStrategy", "trend")
    portfolio_req: SubscribeRequest = create_request("PortfolioStrategy", "trend")
    symbol_contract_map[cta_req.vt_symbol] = Mock()

    try:
        md_api.subscribe(cta_req)
        md_api.subscribe(portfolio_req)

        subscribe_quote.assert_called_once()

        md_api.unsubscribe(cta_req)
        unsubscribe_quote.assert_not_called()
        gateway.on_event.assert_not_called()

        md_api.unsubscribe(portfolio_req)
        unsubscribe_quote.assert_called_once_with(101)
        gateway.on_event.assert_called_once_with(EVENT_TICK_UNSUBSCRIBE, portfolio_req)
        assert not md_api.subscribed
        assert not md_api.subscription_ids
        assert not md_api.subscribers
    finally:
        symbol_contract_map.pop(cta_req.vt_symbol, None)


def test_default_subscriber_can_unsubscribe(monkeypatch: MonkeyPatch) -> None:
    gateway: Mock = Mock()
    gateway.gateway_name = "XT"
    md_api: XtMdApi = XtMdApi(gateway)

    subscribe_quote: Mock = Mock(return_value=102)
    unsubscribe_quote: Mock = Mock()
    monkeypatch.setattr(xtdata, "subscribe_quote", subscribe_quote)
    monkeypatch.setattr(xtdata, "unsubscribe_quote", unsubscribe_quote)

    req: SubscribeRequest = SubscribeRequest("600000", Exchange.SSE)
    symbol_contract_map[req.vt_symbol] = Mock()

    try:
        md_api.subscribe(req)
        md_api.unsubscribe(req)

        unsubscribe_quote.assert_called_once_with(102)
        gateway.on_event.assert_called_once_with(EVENT_TICK_UNSUBSCRIBE, req)
    finally:
        symbol_contract_map.pop(req.vt_symbol, None)


def test_subscription_count_limit(monkeypatch: MonkeyPatch) -> None:
    gateway: Mock = Mock()
    gateway.gateway_name = "XT"
    md_api: XtMdApi = XtMdApi(gateway)

    subscribe_quote: Mock = Mock(return_value=103)
    monkeypatch.setattr(xtdata, "subscribe_quote", subscribe_quote)

    req: SubscribeRequest = create_request("CtaStrategy", "overflow")
    symbol_contract_map[req.vt_symbol] = Mock()
    md_api.subscription_ids = {
        f"existing_{index}": index + 1
        for index in range(MIN_SUBSCRIPTION_COUNT)
    }

    try:
        md_api.subscribe(req)

        subscribe_quote.assert_not_called()
        assert "600000.SH" not in md_api.subscribers
        gateway.write_log.assert_called_once()
    finally:
        symbol_contract_map.pop(req.vt_symbol, None)


def test_gateway_subscription_limit_setting_has_minimum() -> None:
    gateway: XtGateway = XtGateway(EventEngine(), "XT")
    gateway.md_api.connect = Mock()

    setting: dict = {
        "token": "",
        "行情连接": "Token",
        "股票市场": "否",
        "期货市场": "否",
        "期权市场": "否",
        "仿真交易": "否",
        "最大订阅数量": 10,
    }
    gateway._connect(setting)

    gateway.md_api.connect.assert_called_once_with(
        False,
        False,
        False,
        MIN_SUBSCRIPTION_COUNT
    )
    assert XtGateway.default_setting["最大订阅数量"] == DEFAULT_SUBSCRIPTION_COUNT
