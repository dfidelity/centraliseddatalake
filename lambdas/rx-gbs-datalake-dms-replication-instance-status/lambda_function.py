import json
import boto3

def lambda_handler(event, context):
    # TODO implement
    client=boto3.client('dms')
    response = client.describe_replication_instances(
        Filters=[
            {
                'Name': 'replication-instance-arn',
                'Values': [
                   event['replication_instance_arn'],
                ]
            },
        ]
    )
    print(response['ReplicationInstances'][0]['ReplicationInstanceStatus'])
    return response['ReplicationInstances'][0]['ReplicationInstanceStatus']

