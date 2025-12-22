from evm.token_block_message_pb2 import TokenBlockMessage
from collections import defaultdict
from typing import List, Dict, Any

def process_token_block(msg: TokenBlockMessage) -> tuple[List[Dict[str, Any]], int]:
    address_to_latest = defaultdict(lambda: {"balance_obj": None, "max_tx_idx": -1})

    for tx_balances in msg.TransactionBalances:
        for tb in tx_balances.TokenBalances:
            sc = tb.Currency.SmartContract.hex()
            if sc != "":
                continue

            addr = "0x" + tb.Address.hex()
            if tx_balances.TransactionIndex > address_to_latest[addr]["max_tx_idx"]:
                address_to_latest[addr] = {
                    "balance_obj": tb,
                    "max_tx_idx": tx_balances.TransactionIndex,
                }

    results = []
    block_number = bytes_to_uint256(msg.Header.Number)
    block_time = msg.Header.Time
    for addr, data in address_to_latest.items():
        if not data["balance_obj"]:
            continue
        tb = data["balance_obj"]
        balance = to_decimal_string(bytes_to_uint256(tb.PostBalance), tb.Currency.Decimals)
        results.append({
            "address": addr,
            "block_number": block_number,
            "block_timestamp": block_time,
            "balance": balance,
        })
    return results, block_number

def bytes_to_uint256(b: bytes) -> int:
    if len(b) > 32:
        raise ValueError(f"Input too long: {len(b)} bytes > 32 (256 bits)")
    # Convert big-endian bytes to int (unsigned)
    return int.from_bytes(b, byteorder='big')

def to_decimal_string(post_balance: int, decimals: int) -> str:
    if decimals == 0:
        return str(post_balance)
    # Convert to string and pad with leading zeros if needed
    s = str(post_balance).zfill(decimals + 1)  # ensures at least `decimals+1` digits
    # Insert decimal point `decimals` places from the right
    integer_part = s[:-decimals] or '0'
    fractional_part = s[-decimals:].rstrip('0') or '0'
    if fractional_part == '0':
        return integer_part
    return f"{integer_part}.{fractional_part}"