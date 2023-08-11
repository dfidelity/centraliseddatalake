import csv
import json
import codecs
import urllib.parse
import boto3

s3 = boto3.client('s3')


def lambda_handler(event, context):
    #print("Received event: " + json.dumps(event, indent=2))

    bucket = event['Records'][0]['s3']['bucket']['name']
    key = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'], encoding='utf-8')
    directory = "/".join(key.split("/")[:-1])
    config_dir = "/".join(directory.split("/")[:-1]) + "/etl_job_config"
    print("BUCKET -> ", bucket)
    print("KEY -> ", key)
    print("DIRECTORY -> ", directory)
    print("CONFIG DIRECTORY - >", config_dir)
    try:
        result = []
        obj_response = s3.list_objects(Bucket=bucket, Prefix=directory)
        print(obj_response)
        obj_list = obj_response['Contents']
        for obj in obj_list:
            response = s3.get_object(Bucket=bucket, Key=obj['Key'])
            # print("RESPONSE -> ", response)
            # print("CONTENT TYPE: " + response['ContentType'])
            if response['ContentType'] == "text/csv":
                print("File key -> ", obj['Key'])
                for row in csv.DictReader(codecs.getreader('utf-8')(response['Body'])):
                    result.append(row)
        # print("RESULT -> ", result)
        s3.put_object(
             Body=(bytes(json.dumps(result).lstrip("[").rstrip("]").replace("}, {", "},\n{").encode('UTF-8'))),
             Bucket=bucket,
             Key=config_dir+'/etl_job_config.json'
        )
        return result
    except Exception as e:
        print(e)
        raise e
