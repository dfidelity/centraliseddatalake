import json
import boto3

def lambda_handler(event, context):
    # TODO implement
    client=boto3.client('dms')
    response = client.describe_replication_tasks(
        Filters=[
            {
                'Name': 'replication-task-arn',
                'Values': [
                   event['taskname'],
                ]
            },
        ]
    )
    print(event['taskname'], "->", response['ReplicationTasks'][0]['Status'])
    return response['ReplicationTasks'][0]['Status']
