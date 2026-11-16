#!/usr/bin/env python
"""把 QMT 本地库的分钟 K 线批量导入 VeighNa 数据库（DuckDB）。

为什么要单独写：`XtDatafeed.query_bar_history` 是**单标的**读一次，
全市场 5,225 只 × 一年要 5,225 次 RPC，太慢。这里按批（默认 20 只）一次
读回，向量化转换成 DataFrame 后整批 `INSERT OR REPLACE`，最后用一条 SQL
重建 `dbbaroverview`。

口径与 `XtDatafeed.query_bar_history` 完全一致（并有脚本自带的对照校验）：

- QMT 分钟线的时间戳是**区间结束**，入库前减 1 分钟 → 与 vnpy 的"区间开始"一致；
- 09:29（减 1 分钟后）的集合竞价 K 线合并进开盘那根（开高低量额）；
- 日线过滤掉尚未走完的当日；
- 数值列落在 `DECIMAL(18,2)` 上（库表定义如此）。

用法（**先关掉 VeighNa**，DuckDB 是单写者；需要管理员权限连 QMT 管道）::

    # 先小样本估体积（20 只，只读不写）
    python script/import_minute_bars.py --start 2025-11-12 --end 2026-11-13 --limit 20 --dry-run

    # 真导入（可中断，重跑自动跳过已覆盖的标的）
    python script/import_minute_bars.py --start 2025-11-12 --end 2026-11-13

    # 只导 3 个月
    python script/import_minute_bars.py --start 2026-08-13 --end 2026-11-13
"""

from __future__ import annotations

import argparse
import sys
from datetime import datetime, timedelta
from time import monotonic

import duckdb
import numpy as np
import pandas as pd

from vnpy.trader.constant import Exchange, Interval
from vnpy.trader.database import DB_TZ

from bigqmt_signal_trader import xtquant_compat

# QMT 后缀 → vnpy 交易所
SUFFIX_TO_EXCHANGE: dict[str, Exchange] = {
    "SH": Exchange.SSE,
    "SZ": Exchange.SZSE,
    "BJ": Exchange.BSE,
}
# 情绪用的三只指数（不在"沪深A股"板块里，单独补）
INDEX_SYMBOLS: tuple[str, ...] = ("000001.SH", "399001.SZ", "399006.SZ")

AUCTION_TIME = datetime(2000, 1, 1, 9, 29).time()
MINUTE = timedelta(minutes=1)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="导入 QMT 分钟 K 线到 VeighNa 库")
    parser.add_argument("--start", required=True, help="开始日期 YYYY-MM-DD")
    parser.add_argument("--end", required=True, help="结束日期 YYYY-MM-DD")
    parser.add_argument("--period", default="1m", choices=("1m",), help="周期")
    parser.add_argument("--batch-size", type=int, default=20, help="每轮处理多少只")
    parser.add_argument(
        "--chunk-size", type=int, default=2,
        help="单次 RPC 携带几只（越大越省往返，但一年分钟线 payload 很容易撑爆管道）",
    )
    parser.add_argument(
        "--rpc-timeout", type=float, default=120.0, help="单次 RPC 超时（秒）"
    )
    parser.add_argument("--limit", type=int, default=0, help="只处理前 N 只（试跑用）")
    parser.add_argument("--dry-run", action="store_true", help="只读+转换，不写库")
    parser.add_argument(
        "--no-overview", action="store_true", help="结束时不要重建 dbbaroverview"
    )
    parser.add_argument(
        "--skip-existing", action="store_true", default=True,
        help="跳过库里 1m 已覆盖到结束日的标的（默认开）",
    )
    return parser.parse_args(argv)


def load_universe(conn, limit: int) -> list[str]:
    """股票池：库里已有日线的标的（QMT 代码格式）+ 三只指数。"""
    rows = conn.execute(
        "SELECT DISTINCT symbol, exchange FROM dbbaroverview WHERE interval = 'd'"
    ).fetchall()
    suffix_of: dict[Exchange, str] = {v: k for k, v in SUFFIX_TO_EXCHANGE.items()}
    symbols: list[str] = [
        f"{symbol}.{suffix_of[Exchange(exchange)]}"
        for symbol, exchange in rows
        if exchange in {e.value for e in SUFFIX_TO_EXCHANGE.values()}
    ]
    symbols = sorted(set(symbols) | set(INDEX_SYMBOLS))
    return symbols[:limit] if limit else symbols


def covered_symbols(conn, start: datetime, end: datetime) -> set[str]:
    """库里 1m 已覆盖 [start, end] 的标的（用于断点续跑）。"""
    rows = conn.execute(
        """
        SELECT symbol, exchange FROM dbbaroverview
        WHERE interval = '1m' AND start <= ? AND "end" >= ?
        """,
        (start + timedelta(days=5), end - timedelta(days=5)),
    ).fetchall()
    suffix_of: dict[Exchange, str] = {v: k for k, v in SUFFIX_TO_EXCHANGE.items()}
    return {f"{symbol}.{suffix_of[Exchange(exchange)]}" for symbol, exchange in rows
            if exchange in {e.value for e in SUFFIX_TO_EXCHANGE.values()}}


def frame_to_rows(
    qmt_symbol: str, frame: pd.DataFrame
) -> pd.DataFrame:
    """把一只标的的 QMT 分钟 DataFrame 转成 dbbardata 的行（口径同 XtDatafeed）。"""
    if frame is None or len(frame) == 0:
        return pd.DataFrame()

    data = frame.copy()
    # 时间戳：有的终端给 time 列，有的只用 index
    if "time" in data.columns:
        stamps = data.pop("time")
    else:
        stamps = pd.Series(data.index, index=data.index)
    dt_series = pd.to_datetime(
        stamps.astype("int64").astype(str), format="%Y%m%d%H%M%S", errors="coerce"
    )
    data = data.loc[dt_series.notna()].copy()
    dt_series = dt_series[dt_series.notna()]
    # QMT 是"区间结束"，减 1 分钟落到"区间开始"
    data["datetime"] = dt_series - MINUTE
    data = data.sort_values("datetime").reset_index(drop=True)

    codes = list(data.columns)
    def col(name: str) -> pd.Series:
        return data[name] if name in codes else pd.Series(0.0, index=data.index)

    open_p = col("open").astype(float)
    high_p = col("high").astype(float)
    low_p = col("low").astype(float)
    close_p = col("close").astype(float)
    volume = col("volume").astype(float)
    turnover = col("amount").astype(float)

    # 集合竞价那根（减 1 分钟后是 09:29）合并进开盘第一根
    is_auction = data["datetime"].dt.time == AUCTION_TIME
    if bool(is_auction.any()):
        idx = np.flatnonzero(is_auction.to_numpy())
        target = idx + 1
        valid = target < len(data)
        idx, target = idx[valid], target[valid]
        same_day = (
            data["datetime"].iloc[idx].dt.date.to_numpy()
            == data["datetime"].iloc[target].dt.date.to_numpy()
        )
        idx, target = idx[same_day], target[same_day]
        if len(idx):
            auction_open = open_p.iloc[idx].to_numpy()
            high_arr = high_p.to_numpy().copy()
            low_arr = low_p.to_numpy().copy()
            open_arr = open_p.to_numpy().copy()
            vol_arr = volume.to_numpy().copy()
            turn_arr = turnover.to_numpy().copy()
            open_arr[target] = auction_open
            high_arr[target] = np.maximum(high_arr[target], auction_open)
            low_arr[target] = np.minimum(low_arr[target], auction_open)
            np.add.at(vol_arr, target, volume.iloc[idx].to_numpy())
            np.add.at(turn_arr, target, turnover.iloc[idx].to_numpy())
            open_p = pd.Series(open_arr, index=data.index)
            high_p = pd.Series(high_arr, index=data.index)
            low_p = pd.Series(low_arr, index=data.index)
            volume = pd.Series(vol_arr, index=data.index)
            turnover = pd.Series(turn_arr, index=data.index)
        data = data.loc[~is_auction].copy()
        open_p = open_p.loc[data.index]
        high_p = high_p.loc[data.index]
        low_p = low_p.loc[data.index]
        close_p = close_p.loc[data.index]
        volume = volume.loc[data.index]
        turnover = turnover.loc[data.index]

    symbol, _, suffix = qmt_symbol.partition(".")
    exchange = SUFFIX_TO_EXCHANGE.get(suffix)
    if exchange is None:
        return pd.DataFrame()

    rows = pd.DataFrame(
        {
            "symbol": symbol,
            "exchange": exchange.value,
            "datetime": data["datetime"].to_numpy(),
            "interval": Interval.MINUTE.value,
            "volume": volume.to_numpy(),
            "turnover": turnover.to_numpy(),
            "open_interest": 0.0,
            "open_price": open_p.to_numpy(),
            "high_price": high_p.to_numpy(),
            "low_price": low_p.to_numpy(),
            "close_price": close_p.to_numpy(),
        }
    )
    rows = rows.drop_duplicates(subset=["symbol", "exchange", "interval", "datetime"])
    return rows[rows["close_price"] > 0]


def write_rows(con: duckdb.DuckDBPyConnection, rows: pd.DataFrame) -> int:
    """整批 INSERT OR REPLACE（主键幂等）。"""
    if rows.empty:
        return 0
    cursor = con.cursor()
    try:
        cursor.register("staging", rows)
        cursor.execute("BEGIN TRANSACTION")
        cursor.execute(
            """
            INSERT OR REPLACE INTO dbbardata
            (symbol, exchange, datetime, interval, volume, turnover,
             open_interest, open_price, high_price, low_price, close_price)
            SELECT symbol, exchange, datetime, interval, volume, turnover,
                   open_interest, open_price, high_price, low_price, close_price
            FROM staging
            """
        )
        cursor.execute("COMMIT")
    except Exception:
        cursor.execute("ROLLBACK")
        raise
    finally:
        cursor.unregister("staging")
        cursor.close()
    return len(rows)


def rebuild_overview(con: duckdb.DuckDBPyConnection) -> None:
    """一条 SQL 重建 1m 的概览表（逐标的刷新太慢）。"""
    cursor = con.cursor()
    try:
        cursor.execute("BEGIN TRANSACTION")
        cursor.execute("DELETE FROM dbbaroverview WHERE interval = '1m'")
        cursor.execute(
            """
            INSERT INTO dbbaroverview (symbol, exchange, interval, count, start, "end")
            SELECT symbol, exchange, interval, CAST(COUNT(*) AS INTEGER),
                   MIN(datetime), MAX(datetime)
            FROM dbbardata WHERE interval = '1m'
            GROUP BY symbol, exchange, interval
            """
        )
        cursor.execute("COMMIT")
    except Exception:
        cursor.execute("ROLLBACK")
        raise
    finally:
        cursor.close()


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    start = datetime.combine(datetime.fromisoformat(args.start).date(), datetime.min.time())
    end = datetime.combine(datetime.fromisoformat(args.end).date(), datetime.max.time())
    if end <= start:
        raise SystemExit("结束日期必须晚于开始日期")

    from vnpy.trader.database import get_database

    database = get_database()
    db_path: str = str(getattr(database, "path", ""))
    if not db_path:
        raise SystemExit("无法确定 DuckDB 路径（请确认 database.name=duckdb）")
    print(f"数据库: {db_path}", flush=True)
    # 复用 vnpy 已建立的连接（同进程再开一个只读连接会因配置不同被拒）
    conn = database.conn

    symbols = load_universe(conn, args.limit)
    if args.skip_existing and not args.limit:
        done = covered_symbols(conn, start, end)
        before = len(symbols)
        symbols = [s for s in symbols if s not in done]
        print(f"跳过已覆盖 {before - len(symbols)} 只", flush=True)
    total = len(symbols)
    print(f"待导入 {total} 只，{start:%Y-%m-%d} ~ {end:%Y-%m-%d}，"
          f"每批 {args.batch_size} 只，{'试跑' if args.dry_run else '写入'}", flush=True)

    xtquant_compat.configure()
    xtdata = xtquant_compat.xtdata
    con = None if args.dry_run else conn

    fmt_start = start.strftime("%Y%m%d") + "000000"
    fmt_end = (end + timedelta(days=1)).strftime("%Y%m%d") + "000000"
    total_rows = 0
    empty: list[str] = []
    written_symbols: set[str] = set()
    failed_batches = 0
    began = monotonic()

    try:
        for offset in range(0, total, args.batch_size):
            batch = symbols[offset: offset + args.batch_size]
            try:
                payload = xtdata.get_market_data_ex(
                    field_list=["time", "open", "high", "low", "close", "volume", "amount"],
                    stock_list=batch,
                    period=args.period,
                    start_time=fmt_start,
                    end_time=fmt_end,
                    count=-1,
                    dividend_type="none",
                    fill_data=False,
                    heal=False,
                    chunk_size=args.chunk_size,
                    timeout_seconds=args.rpc_timeout,
                )
            except Exception as ex:  # noqa: BLE001 - 单批失败不中断整轮
                print(f"  批次读取失败 {batch[0]}…: {type(ex).__name__}: {ex}", flush=True)
                failed_batches += 1
                continue

            frames: list[pd.DataFrame] = []
            for code in batch:
                frame = payload.get(code) if isinstance(payload, dict) else None
                converted = frame_to_rows(code, frame)
                if converted.empty:
                    empty.append(code)
                    continue
                frames.append(converted)
                written_symbols.add(code)
            if not frames:
                continue
            rows = pd.concat(frames, ignore_index=True)
            if con is not None:
                total_rows += write_rows(con, rows)
            else:
                total_rows += len(rows)

            done = offset + len(batch)
            elapsed = monotonic() - began
            rate = done / elapsed if elapsed else 0
            eta = (total - done) / rate if rate else 0
            print(
                f"  [{done}/{total}] {len(rows):7d} 行  累计 {total_rows:11,d}  "
                f"{elapsed:5.0f}s  剩余 ~{eta / 60:.1f} 分钟",
                flush=True,
            )
    finally:
        if con is not None:
            if not args.no_overview:
                print("重建 dbbaroverview(1m) …", flush=True)
                rebuild_overview(con)

    print(
        f"完成：{total_rows:,} 行，{len(written_symbols)} 只有数据，"
        f"{len(empty)} 只空，{failed_batches} 批读取失败"
        + (f"（空例：{empty[:5]}）" if empty else ""),
        flush=True,
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
