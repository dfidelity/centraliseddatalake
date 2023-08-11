import json
import boto3

def lambda_handler(event, context):
    conn = boto3.client("emr", region_name='eu-west-1')#event['emrId']
    response = conn.describe_step(
        ClusterId=event['emrId'],
        StepId=event['emrJobId']
    )
    jobstatus=str(response['Step']['Status']['State'])
    return jobstatus
