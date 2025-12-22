from evm.token_block_message_pb2 import TokenBlockMessage

def decode_token_block_message(raw_bytes: bytes) -> TokenBlockMessage:
    msg = TokenBlockMessage()
    msg.ParseFromString(raw_bytes)
    return msg
