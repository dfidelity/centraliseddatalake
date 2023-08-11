import json
import boto3

def lambda_handler(event, context):
    client=boto3.client('dms')
    print(event)
    response = client.modify_replication_instance(
        ReplicationInstanceArn=event['replication_instance_arn'],
        ApplyImmediately=event['upgrade_status'],
        ReplicationInstanceClass=event['upgrade_instance_class'],
    )
    return response['ReplicationInstance']['ReplicationInstanceStatus']
