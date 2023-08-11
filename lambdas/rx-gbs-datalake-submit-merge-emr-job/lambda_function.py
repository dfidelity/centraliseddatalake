import json
import boto3

def lambda_handler(event, context):
    conn = boto3.client("emr", region_name='eu-west-1')#event['emrId']

    response_run_code =  conn.add_job_flow_steps(JobFlowId=event['emrId'],
                                 Steps=[
                                     {
                                         'Name': 'AWS RUN',
                                         'ActionOnFailure': 'CONTINUE',
                                         'HadoopJarStep': {
                                             'Jar': 'command-runner.jar',
                                             'Args':["spark-submit","--master","yarn",
                                                    "--jars","/usr/lib/spark/jars/httpclient-4.5.9.jar,/usr/share/aws/aws-java-sdk/aws-java-sdk-bundle-1.12.31.jar,/usr/lib/hudi/hudi-spark-bundle.jar,/usr/lib/spark/external/lib/spark-avro.jar",
                                                    "--conf","spark.sql.hive.convertMetastoreParquet=false",
                                                    "--py-files",event['modulesPath'],event['filePathRefined'],
                                                    event['env_id'],event['sns_arn'],event['configDB'],
                                                    event['configTable'],event['logsTable'],event['logHistoryS3Path'],
                                                    event['groupId']]
                                         }
                                     }
                                 ],
                            )    
    job_id=str(response_run_code['StepIds'][0])                        
    return job_id#response_run_code#event['emrId']
