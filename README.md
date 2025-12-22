# Bitquery Token Stream Processor

Consumes `eth.tokens.proto` Kafka stream, filters TokenBalances with specific `smart_contract`, 
keeps latest per address by `transaction_index`, writes to DynamoDB keyed by lowercase `0x...` 
address — only if `block_number` > existing.

## Local Dev
```bash
docker-compose up --build
# DynamoDB: http://localhost:8000/shell
