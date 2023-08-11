import json
import boto3

def lambda_handler(event, context):
    client = boto3.client('sns')
    print(event['emrId'])
    message = {
        "groupId": event['groupId'],
        "emrId": event['emrId'],
        "status": "FAILED",
        "message": "Failure in step function execution steps"
    }
    response = client.publish(
        TopicArn=event['sns_arn'],
        Message=json.dumps({'default': json.dumps(message),
                        'email': 'Failure: '+event['emrId']+'\n\nPlease follow the following steps:\n1.Check the execution of the step function rx-gbs-datalake-submit-emr-rawzone or rx-gbs-datalake-submit-emr-refinedzone\n2.Check the '+event['emrId']+' EMR cluster \n3.Check the logs and error message'}),
        Subject=event['env_id'] + ': Failure in step function execution steps',
        MessageStructure='json'
    )
    
    # Print out the response
    print(response)
    return event['emrId']
