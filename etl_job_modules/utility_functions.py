from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext
from pyspark.sql import SparkSession
from pyspark.sql import HiveContext
import boto3
import json
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import *
from datetime import datetime


## udf to transformed the dataframe and produce a new DF with non sensitive anonymized data.
#not added try except here as it will get handled when it is called
def anonymize_pii_data(column_name, stagingDF):
    # #hash the column using sha256
    #debugging specific
    print("*****STARTED anonymize_pii_data*****")
    data_type_list = dict(stagingDF.dtypes)
    if column_name in data_type_list:
        data_type = data_type_list[column_name]
        if data_type == 'string':
            hashed_df = stagingDF.withColumn('anonymized_column', sha2(stagingDF[column_name], 256))
        else:
            hashed_df = stagingDF.withColumn('anonymized_column', bin((stagingDF[column_name]*0.3)/2.7))
        # #drop the exiting column with sensitive data
        transformed_df = hashed_df.drop(column_name)
        # rename the column back to hashed column
        anonymized_df = transformed_df.withColumnRenamed('anonymized_column', column_name)
        #debugging specific
        print("*****COMPLETED anonymize_pii_data*****")
        return anonymized_df
    return stagingDF


def send_notification(sns_arn, n_type, message, step_name,group_id=''):
    # v_initial_failure_lambda_sns_arn
    # v_table_load_failure_lambda_sns_arn
    # need to get from secret manager
    # v_initial_failure_lambda_sns_arn = 'arn:aws:sns:eu-west-1:337962473469:rx-uk-datalake-step-function-emr-alerts'
    # v_table_load_failure_lambda_sns_arn = 'arn:aws:sns:eu-west-1:337962473469:rx-uk-datalake-step-function-emr-alerts'

    if n_type == 'initial':
        print("Sending error notification!")
        print("ERROR MESSAGE",message)
        client = boto3.client('sns')
        message_dict = {
            "groupId": group_id,
            "status": "FAILED",
            "message": "ERROR in etl : " + str(message)
        }
        response = client.publish(
            TopicArn=sns_arn,
            Message=json.dumps({'default': json.dumps(message_dict),
                                'email': message_dict['message']}),
            Subject='ERROR in ETL',
            MessageStructure='json'
        )
        # response = client.invoke(
        #     FunctionName=v_initial_failure_lambda_sns_arn,#'arn:aws:lambda:eu-west-1:376980907058:function:rx-gbs-datalake-emr-code-initial-failure-send-sns-notification',
        #     InvocationType='Event',
        #     Payload=json.dumps(error_message)
        # )
    if n_type == 'final':
        print("Sending error notification!")
        #table_names={"table_names":str(message)}
        #payload = json.dumps(table_names)
        #print("TABLE NAMES which had ERROR: ",table_names)
        client = boto3.client('sns')
        response = client.publish(
            TopicArn=sns_arn,
            Message=json.dumps({'default': json.dumps(message),
                                'email': message['message']}),
            Subject='Failure in ETL',
            MessageStructure='json'
        )
        # response = client.invoke(
        #     FunctionName=v_table_load_failure_lambda_sns_arn,#'arn:aws:lambda:eu-west-1:376980907058:function:rx-gbs-datalake-emr-failure-table-load-send-sns-notification',
        #     InvocationType='Event',
        #     Payload=json.dumps(table_names)
        # )
    if n_type == 'success':
        print("Sending success notification!")
        client = boto3.client('sns')
        response = client.publish(
            TopicArn=sns_arn,
            Message=json.dumps({'default': json.dumps(message)}),
            MessageStructure='json'
        )
    #debugging specific
    print("*************************NOTIFICATION SENT****************************")
    return
