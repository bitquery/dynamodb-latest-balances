from evm.token_block_message_pb2 import TokenBlockMessage
from collections import defaultdict
from typing import List, Dict, Any

def process_token_block(msg: TokenBlockMessage) -> List[Dict[str, Any]]:
    address_to_latest = defaultdict(lambda: {"balance_obj": None, "max_tx_idx": -1})

    for tx_balances in msg.transactions:
        for tb in tx_balances.balances:
            if tb.smart_contract.strip() != "":
                continue

            addr = tb.address.lower()
            if tb.transaction_index > address_to_latest[addr]["max_tx_idx"]:
                address_to_latest[addr] = {
                    "balance_obj": tb,
                    "max_tx_idx": tb.transaction_index,
                }

    results = []
    for addr, data in address_to_latest.items():
        if not data["balance_obj"]:
            continue
        tb = data["balance_obj"]
        try:
            divisor = 10 ** tb.decimals
            balance_float = tb.amount / divisor
        except (OverflowError, ZeroDivisionError):
            balance_float = float(tb.amount)

        results.append({
            "address": f"0x{addr.removeprefix('0x')}",
            "block_number": msg.block_number,
            "block_timestamp": msg.block_timestamp,
            "balance": balance_float,
        })

    return results
