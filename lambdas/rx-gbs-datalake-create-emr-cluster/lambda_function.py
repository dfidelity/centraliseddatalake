import os
import boto3


def lambda_handler(event, context):
    conn = boto3.client("emr", region_name='eu-west-1')
    cluster_id = conn.run_job_flow(
        Name='rx-gbs-datalake-emr-cluster',
        ServiceRole=os.environ['emr_service_role'],
        JobFlowRole=os.environ['emr_service_role'],
        VisibleToAllUsers=True,
        LogUri='s3://' + os.environ['logs_bucket_name'] + '/elasticmapreduce/',
        ReleaseLabel='emr-6.5.0',
        Instances={
            "Ec2SubnetId": os.environ['emr_subnet_id'],
            'InstanceGroups': [
                {
                    'Name': 'Master - 1',
                    'Market': 'ON_DEMAND',
                    'InstanceRole': 'MASTER',
                    'InstanceType': 'm5.4xlarge',
                    'InstanceCount': 1,
                    'EbsConfiguration': {
                        'EbsBlockDeviceConfigs': [
                            {
                                'VolumeSpecification':
                                    {
                                        'SizeInGB': 32,
                                        'VolumeType': 'gp2'
                                    },
                                'VolumesPerInstance': 2
                            }
                        ]
                    },
                    'Configurations': [
                        {
                            'Classification': 'yarn-site',
                            'Properties':
                                {
                                    'yarn.nodemanager.pmem-check-enabled': 'false',
                                    'yarn.scheduler.maximum-allocation-mb': '20000',
                                    'yarn.nodemanager.vmem-check-enabled': 'false'
                                }
                        }
                    ],
                },
                {
                    'Name': 'Core - 2',
                    'Market': 'ON_DEMAND',
                    'InstanceRole': 'CORE',
                    'InstanceType': 'm5.4xlarge',
                    'InstanceCount': 4,
                    'EbsConfiguration': {
                        'EbsBlockDeviceConfigs': [
                            {
                                'VolumeSpecification':
                                    {
                                        'SizeInGB': 32,
                                        'VolumeType': 'gp2'
                                    },
                                'VolumesPerInstance': 2
                            }
                        ]
                    },
                    'Configurations': [
                        {
                            'Classification': 'yarn-site',
                            'Properties':
                                {
                                    'yarn.nodemanager.pmem-check-enabled': 'false',
                                    'yarn.scheduler.maximum-allocation-mb': '20000',
                                    'yarn.nodemanager.vmem-check-enabled': 'false'
                                }
                        }
                    ],
                }
            ],
            'KeepJobFlowAliveWhenNoSteps': True,
            'Ec2KeyName': os.environ['ec2_key_name'],
            'TerminationProtected': False,
            'EmrManagedMasterSecurityGroup': os.environ['emr_master_sg'],
            'EmrManagedSlaveSecurityGroup': os.environ['emr_slave_sg'],
            'ServiceAccessSecurityGroup': os.environ['emr_service_access_sg'],

        },
        Applications=[{'Name': 'Hadoop'}, {'Name': 'Spark'}, {'Name': 'Livy'}, {'Name': 'Ganglia'}, {'Name': 'Hue'},
                      {'Name': 'Hive'}, {'Name': 'Presto'}],
        BootstrapActions=[{'Name': 'Custom action', 'ScriptBootstrapAction': {'Path': os.environ['bootstrap_action_path']}}],
        ScaleDownBehavior='TERMINATE_AT_TASK_COMPLETION',
        SecurityConfiguration=os.environ['emr_security_config'],
        EbsRootVolumeSize=100,
        AutoScalingRole=os.environ['emr_service_role'],
        StepConcurrencyLevel=10,
        Steps=[],
        Configurations=[{"Classification": "spark-defaults",
                         "Properties":
                             {
                                 "spark.network.timeout": "42000s",
                                 "spark.executor.heartbeatInterval": "60s",
                                 "spark.driver.memory": "16000M",
                                 "spark.driver.maxResultSize": "10g",
                                 "spark.executor.memory": "16000M",
                                 "spark.executor.cores": "5",
                                 "spark.executor.instances": "20",
                                 "spark.yarn.executor.memoryOverhead": "3000M",
                                 "spark.yarn.driver.memoryOverhead": "3000M",
                                 "spark.executor.defaultJavaOptions": "-XX:+UseG1GC -XX:+UnlockDiagnosticVMOptions -XX:+G1SummarizeConcMark -XX:InitiatingHeapOccupancyPercent=35 -verbose:gc -XX:+PrintGCDetails -XX:+PrintGCDateStamps -XX:OnOutOfMemoryError='kill -9 %p'",
                                 "spark.driver.defaultJavaOptions": "-XX:+UseG1GC -XX:+UnlockDiagnosticVMOptions -XX:+G1SummarizeConcMark -XX:InitiatingHeapOccupancyPercent=35 -verbose:gc -XX:+PrintGCDetails -XX:+PrintGCDateStamps -XX:OnOutOfMemoryError='kill -9 %p'",
                                 "spark.storage.level": "MEMORY_AND_DISK_SER",
                                 "spark.rdd.compress": "true",
                                 "spark.shuffle.compress": "true",
                                 "spark.shuffle.spill.compress": "true",
                                 "spark.default.parallelism": "200",
                                 "spark.sql.shuffle.partitions": "200",
                                 "spark.sql.files.maxPartitionBytes": "128000000",
                                 "spark.sql.files.openCostInBytes": "4194304",
                                 "spark.sql.broadcastTimeout": "4200",
                                 "spark.sql.autoBroadcastJoinThreshold": "104857600",
                                 "spark.serializer": "org.apache.spark.serializer.KryoSerializer",
                                 "spark.dynamicAllocation.executorIdleTimeout": "3600s",
                                 "spark.dynamicAllocation.enabled": "false",
                                 "spark.sql.legacy.parquet.datetimeRebaseModeInRead": "LEGACY",
                                 "spark.sql.legacy.parquet.datetimeRebaseModeInWrite": "CORRECTED",
                                 "spark.sql.legacy.parquet.int96RebaseModeInRead": "LEGACY",
                                 "spark.sql.legacy.parquet.int96RebaseModeInWrite": "CORRECTED"
                             }
                         },
                        {"Classification": "yarn-site",
                         "Properties":
                             {
                                 "yarn.log-aggregation.retain-seconds": "-1",
                                 "yarn.nodemanager.resource.cpu-vcores": "5",
                                 "yarn.log-aggregation-enable": "true",
                                 "yarn.nodemanager.pmem-check-enabled": "false",
                                 "yarn.nodemanager.remote-app-log-dir": 's3://' + os.environ[
                                     'logs_bucket_name'] + '/elasticmapreduce/',
                                 "yarn.scheduler.maximum-allocation-mb": "40000",
                                 "yarn.nodemanager.vmem-check-enabled": "false",
                                 "yarn.nodemanager.resource.memory-mb": "40000"
                             }
                         },
                        {"Classification": "hive-site",
                         "Properties":
                             {
                                 "hive.metastore.schema.verification": "false",
                                 "hive.metastore.client.factory.class": "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory"
                             }
                         },
                        {"Classification": "spark-hive-site",
                         "Properties":
                             {
                                 "hive.metastore.client.factory.class": "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory"
                             }
                         }],
        Tags=[
            {
                'Key': 'rx:billing:pm-project-code',
                'Value': 'n/a',
            },
            {
                'Key': 'rx:billing:finance-activity-id',
                'Value': '8000',
            },
            {
                'Key': 'rx:billing:finance-entity-id',
                'Value': '92',
            },
            {
                'Key': 'rx:billing:finance-management-centre-id',
                'Value': '99440',
            },
            {
                'Key': 'rx:billing:jira-project-code',
                'Value': 'RDL',
            },
            {
                'Key': 'rx:billing:pm-programme',
                'Value': 'platform',
            },
            {
                'Key': 'rx:billing:environment-name',
                'Value': 'core-prod',
            },
            {
                'Key': 'Name',
                'Value': 'rx-gbs-datalake-emr-cluster',
            }
        ],
    )
    return cluster_id['JobFlowId']
