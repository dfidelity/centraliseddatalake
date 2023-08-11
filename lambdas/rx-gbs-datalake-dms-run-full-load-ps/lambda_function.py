import json
import boto3

def lambda_handler(event, context):
    # TODO implement
    client=boto3.client('dms')
    response = client.start_replication_task(
        ReplicationTaskArn=event['taskname'],
        StartReplicationTaskType='reload-target'
    )
    return event['taskname']
