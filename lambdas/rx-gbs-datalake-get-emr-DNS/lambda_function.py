import json
import boto3

def lambda_handler(event, context):
    conn = boto3.client("emr", region_name='eu-west-1')
    response = conn.describe_cluster(ClusterId=event['emrId'])
    print(response)
    emrURL= response['Cluster']['MasterPublicDnsName']
    return emrURL
