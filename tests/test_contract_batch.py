from unittest import TestCase
from unittest.mock import Mock, patch

import vnpy_xt.xt_gateway as xt_gateway
from vnpy_xt.xt_gateway import XtMdApi, symbol_contract_map, symbol_limit_map
from vnpy_xt.xt_gateway import xtdata


class ContractBatchTest(TestCase):
    def test_stock_contract_query_uses_bounded_detail_batches(self) -> None:
        gateway: Mock = Mock()
        gateway.gateway_name = "XT"
        md_api: XtMdApi = XtMdApi(gateway)
        symbols: list[str] = ["600000.SH", "000001.SZ", "000002.SZ"]

        def get_details(stockcodes: list[str], timeout_seconds: float) -> dict:
            return {
                code: {
                    "InstrumentName": code,
                    "VolumeMultiple": 1,
                    "PriceTick": 0.01,
                    "UpStopPrice": 100.0,
                    "DownStopPrice": 1.0,
                }
                for code in stockcodes
            }

        batch_call: Mock = Mock(side_effect=get_details)
        single_call: Mock = Mock()
        with (
            patch.object(xt_gateway, "CONTRACT_DETAIL_BATCH_SIZE", 2),
            patch.object(
                xtdata,
                "get_stock_list_in_sector",
                Mock(side_effect=[symbols, []]),
            ),
            patch.object(
                xtdata,
                "get_instrument_detail_batch",
                batch_call,
                create=True,
            ),
            patch.object(xtdata, "get_instrument_detail", single_call),
        ):
            try:
                md_api.query_stock_contracts()
            finally:
                for call in gateway.on_contract.call_args_list:
                    contract = call.args[0]
                    symbol_contract_map.pop(contract.vt_symbol, None)
                    symbol_limit_map.pop(contract.vt_symbol, None)

        self.assertEqual(
            [call.args[0] for call in batch_call.call_args_list],
            [symbols[:2], symbols[2:]],
        )
        self.assertTrue(all(
            call.kwargs["timeout_seconds"]
            == xt_gateway.CONTRACT_DETAIL_TIMEOUT_SECONDS
            for call in batch_call.call_args_list
        ))
        single_call.assert_not_called()
        self.assertEqual(gateway.on_contract.call_count, len(symbols))
