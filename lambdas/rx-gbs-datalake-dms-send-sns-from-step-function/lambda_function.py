import boto3

def lambda_handler(event, context):
    client = boto3.client('sns')
    response = client.publish(
        TopicArn=event['sns_arn'],
        Message='Failure: '+event['taskname']+'\n\nPlease follow the following steps:\n1.Check the execution of the step function rx-gbs-datalake-submit-emr-rawzone or rx-gbs-datalake-submit-emr-refinedzone\n2.Check the '+event['taskname']+' DMS TASK ARN \n3.Check the logs and error message',

        Subject='UAT : Failure in step function execution steps',
    )
    
    # Print out the response
    print(response)
    return event['taskname']
