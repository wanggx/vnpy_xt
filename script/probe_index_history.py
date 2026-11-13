#!/usr/bin/env python
"""排查指数 K 线为什么下不到：4 种组合逐个试，打印行数。

在同一台跑着大 QMT 的机器上执行（需要 QMT 已登录、桥可用）::

    python script/probe_index_history.py
    python script/probe_index_history.py --start 20261013 --end 20261113

对每只指数、每个周期（1d / 1m）分别用 ``dividend_type="none"`` 与
``"front_ratio"`` 查一次：

- ``none`` 有数据、``front_ratio`` 空 → 就是复权口径的问题（vnpy_xt 已修：指数自动用 none）
- 两者都空 → QMT 本地没有这批指数数据，需要在 QMT 终端「数据管理」补充该周期
- 报 TransportError → 桥没连上（大 QMT 侧策略没起 / transport 配置不对）
"""

from __future__ import annotations

import argparse
from datetime import date, timedelta

from bigqmt_signal_trader import xtquant_compat

INDEX_SYMBOLS: tuple[str, ...] = ("000001.SH", "399001.SZ", "399006.SZ")
PERIODS: tuple[str, ...] = ("1d", "1m")
DIVIDEND_TYPES: tuple[str, ...] = ("none", "front_ratio")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="排查指数 K 线可取性")
    parser.add_argument(
        "--start", default=(date.today() - timedelta(days=30)).strftime("%Y%m%d")
    )
    parser.add_argument("--end", default=date.today().strftime("%Y%m%d"))
    args = parser.parse_args(argv)

    xtquant_compat.configure()
    xtdata = xtquant_compat.xtdata

    print(f"区间 {args.start} ~ {args.end}")
    rows: list[tuple[str, str, str, str]] = []
    for symbol in INDEX_SYMBOLS:
        for period in PERIODS:
            for dividend_type in DIVIDEND_TYPES:
                try:
                    data = xtdata.get_market_data_ex(
                        field_list=["time", "open", "high", "low", "close", "volume"],
                        stock_list=[symbol],
                        period=period,
                        start_time=args.start,
                        end_time=args.end,
                        count=-1,
                        dividend_type=dividend_type,
                        fill_data=False,
                    )
                except Exception as ex:  # noqa: BLE001 - 逐项试，失败也要继续
                    detail = f"{type(ex).__name__}: {ex}"
                    rows.append((symbol, period, dividend_type, f"异常 {detail[:60]}"))
                    print(f"{symbol:12s} {period:3s} {dividend_type:12s} 异常 {detail[:80]}")
                    continue
                frame = data.get(symbol) if isinstance(data, dict) else None
                if frame is None or frame.empty:
                    rows.append((symbol, period, dividend_type, "0 行"))
                    print(f"{symbol:12s} {period:3s} {dividend_type:12s} 0 行")
                    continue
                first = str(frame.index[0])
                last = str(frame.index[-1])
                rows.append((symbol, period, dividend_type, str(len(frame))))
                print(
                    f"{symbol:12s} {period:3s} {dividend_type:12s} {len(frame):6d} 行"
                    f"  {first} ~ {last}"
                )

    print()
    print("结论提示：")

    def has_data(count: str) -> bool:
        return count.isdigit() and int(count) > 0

    def has_error(count: str) -> bool:
        return count.startswith("异常")

    has_none = any(
        dtype == "none" and has_data(count) for *_, dtype, count in rows
    )
    has_front = any(
        dtype == "front_ratio" and has_data(count) for *_, dtype, count in rows
    )
    all_error = all(has_error(count) for *_, count in rows)
    if has_none and not has_front:
        print("  none 有数据、front_ratio 为空 → 复权口径问题（vnpy_xt 已改为指数用 none）")
    elif all_error:
        print("  全部异常 → 先解决桥连接（大 QMT 侧策略/transport）")
    elif not has_none:
        print("  none 也拿不到 → QMT 本地没有这些指数的数据，去 QMT 终端「数据管理」补充 1d/1m")
    else:
        print("  两种复权都有数据 → 与复权无关，检查 vnpy_xt 日志里的请求参数")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
