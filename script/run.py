import os
import sys

# python script/run.py 时 sys.path[0] 是 script/，找不到仓库根下的
# 从 xtquant-big-convert 拷来的 bigqmt_signal_trader / xtquant。
_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)

from vnpy.event import EventEngine
from vnpy.trader.engine import MainEngine
from vnpy.trader.ui import MainWindow, create_qapp

from vnpy_xt import XtGateway
from vnpy_datamanager import DataManagerApp


# 历史数据走大 QMT RPC，不再配置迅投研 token。
# 需 xtquant-big-convert 与 QMT 端 BIGQMT_REDIS_DRYRUN 已运行。


def main():
    """主入口函数"""
    qapp = create_qapp()

    event_engine = EventEngine()
    main_engine = MainEngine(event_engine)
    main_engine.add_gateway(XtGateway)
    main_engine.add_app(DataManagerApp)

    main_window = MainWindow(main_engine, event_engine)
    main_window.showMaximized()

    qapp.exec()


if __name__ == "__main__":
    main()
