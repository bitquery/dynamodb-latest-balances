# Bitquery Token Stream Processor

Consumes `eth.tokens.proto` Kafka stream, filters TokenBalances with specific `smart_contract`, 
keeps latest per address by `transaction_index`, writes to DynamoDB keyed by lowercase `0x...` 
address — only if `block_number` > existing.

## Local Dev

create copy of config/kafka_config_template.yaml as config/kafka_config.local.yaml
edit it to set user names and password and save, do not commit to git!

```bash

python main.py --config=config/kafka_config.local.yaml

```

## Deploy to AWS

### Pre-requisites

  - AWS CLI configured (aws configure)
  - Docker (optional, for building image)
  - Git, python3.11, pip
  - Bitquery Kafka credentials (username/password, topic: eth.tokens.proto)




### Create secure storage in AWS

Create copy of config/kafka_config_template.yaml as config/kafka_config.yaml
edit it to set user names and password and save, do not commit to git!

```
aws ssm put-parameter \
  --name "/bitquery/kafka-config" \
  --type "SecureString" \
  --value "file://config/kafka_config.yaml" \
  --region us-east-1
```

returns

```
{
    "Version": 1,
    "Tier": "Standard"
}
```

### Build & Push Docker Image to ECR

```
aws ecr create-repository --repository-name dynamodb-latest-balances --region us-east-1
```

returns

```
{
    "repository": {
        "repositoryArn": "arn:aws:ecr:us-east-1:782919790476:repository/dynamodb-latest-balances",
        "registryId": "782919790476",
        "repositoryName": "dynamodb-latest-balances",
        "repositoryUri": "782919790476.dkr.ecr.us-east-1.amazonaws.com/dynamodb-latest-balances",
        "createdAt": "2025-12-23T09:34:42.705000+02:00",
        "imageTagMutability": "MUTABLE",
        "imageScanningConfiguration": {
            "scanOnPush": false
        },
        "encryptionConfiguration": {
            "encryptionType": "AES256"
        }
    }
}
```

```shell

ACCOUNT=$(aws sts get-caller-identity --query Account --output text)
REGION=us-east-1
REPO=$ACCOUNT.dkr.ecr.$REGION.amazonaws.com/dynamodb-latest-balances:latest

aws ecr get-login-password --region $REGION | docker login --username AWS --password-stdin $REPO

docker buildx build --platform linux/amd64 -t dynamodb-latest-balances .
docker tag dynamodb-latest-balances:latest $REPO
docker push $REPO
```

### Create VPC in AWS console

Create VPC together with subnet(s), NAT gateway for publi Inet access

Create CF stack, substitute vpc IDs (vpc-xxxxxxxx) and public subnet ID(subnet-aaaaaaa) :

```
aws cloudformation deploy \
  --template-file template.yaml \
  --stack-name dynamodb-latest-balances-prod \
  --parameter-overrides \
    VpcId=vpc-xxxxxxxx \
    PublicSubnetId=subnet-aaaaaaa \
  --capabilities CAPABILITY_IAM \
  --region us-east-1
```


returns

```
Waiting for changeset to be created..
Waiting for stack create/update to complete
Successfully created/updated stack - dynamodb-latest-balances-prod
```


### Check instances running

```
aws ec2 describe-instances \
  --filters "Name=tag:aws:cloudformation:stack-name,Values=dynamodb-latest-balances-prod" \
  --query 'Reservations[*].Instances[*].[InstanceId,State.Name,PublicIpAddress,LaunchTime]' \
  --output table
-------------------------------------------------------------------------
|                           DescribeInstances                           |
+----------------------+----------+-------+-----------------------------+
|  i-046d1f7c9b266095b |  running |  None |  2025-12-23T07:30:25+00:00  |
+----------------------+----------+-------+-----------------------------+
```

Ssh if needed

```
aws ssm start-session --target
```


Logs

```
aws logs describe-log-streams \
  --log-group-name dynamodb-latest-balances-prod-logs \
  --region us-east-1
```

## Reverting deployment


Note that all DynamoDb data will be permanently removed!

```
aws cloudformation delete-stack \
  --stack-name dynamodb-latest-balances-prod \
  --region us-east-1
```