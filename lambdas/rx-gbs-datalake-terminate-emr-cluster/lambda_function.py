import boto3

def lambda_handler(event, context):
    conn = boto3.client("emr", region_name='eu-west-1')
    
    if isinstance(event, list):
        conn.terminate_job_flows(JobFlowIds=[event[0]['emrId']])
    else:
        conn.terminate_job_flows(JobFlowIds=[event['emrId']])
