#addin nuget:?package=AWSSDK.AutoScaling&version=3.3.100.7
#addin nuget:?package=AWSSDK.CloudFormation&version=3.3.100.37
#addin nuget:?package=AWSSDK.Core&version=3.3.103.1
#addin nuget:?package=AWSSDK.EC2&version=3.3.117.3
#addin nuget:?package=AWSSDK.ElasticBeanstalk&version=3.3.100.37
#addin nuget:?package=AWSSDK.S3&version=3.3.102.18
#addin nuget:?package=AWSSDK.SecurityToken&version=3.3.101.19
#addin nuget:?package=SharpZipLib&version=1.3.3
#addin nuget:?package=Firefly.CrossPlatformZip&version=0.5.0
#addin nuget:?package=YamlDotNet&version=6.0.0
#addin nuget:?package=AWSSDK.Lambda&version=3.3.102.19
#addin nuget:?package=AWSSDK.KeyManagementService&version=3.3.101.23
#addin nuget:?package=Polly&version=7.2.0

#addin nuget:?package=ReedExpo.Cake.Base&version=1.0.7
#addin nuget:?package=ReedExpo.Cake.AWS.Base&version=1.0.25
#addin nuget:?package=Cake.FileHelpers&version=3.2.0
#addin nuget:?package=ReedExpo.Cake.AWS.BuildAuthentication&version=1.0.18
#addin nuget:?package=ReedExpo.Cake.AWS.CloudFormation&version=2.0.111
#addin nuget:?package=Cake.AWS.S3&version=0.6.8
#addin nuget:?package=AWSSDK.Core&version=3.3.103.15
#addin nuget:?package=AWSSDK.Glue&version=3.3.106
#addin nuget:?package=AWSSDK.Athena&version=3.3.100.51
#addin nuget:?package=ReedExpo.Cake.AWS.Lambda&version=1.1.25

#addin nuget:?package=Cake.Http&version=0.6.1
#addin nuget:?package=ReedExpo.Cake.Cyclonedx&version=1.0.12
#addin nuget:?package=Newtonsoft.Json&version=13.0.1
#addin nuget:?package=ReedExpo.Cake.CrossPlatformZip&version=1.0.18
#addin nuget:?package=ReedExpo.Cake.ServiceNow&version=1.0.27
#addin nuget:?package=Firefly.EmbeddedResourceLoader&version=0.1.3

using System;
using System.Threading;
using Amazon.Lambda;
using Amazon.Runtime;
using Amazon.Glue;
using Amazon.Glue.Model;
using Amazon.Runtime.Internal;
using Amazon.Athena;
using Amazon.Athena.Model;
using Newtonsoft.Json;
using System.Collections.Generic;

AWSCredentials awsCredentials;
RegionEndpoint region = RegionEndpoint.EUWest1;
string codeBucketName;
string environment;
string deploymentRole;
string PowerBIRDPSource;
string EMRMasterSSHSourceIP;
string JumpBoxSG;
string EDWSourceDB;
string EDWSourceDBEngine;
string EDWSourceDBServerName;
string EDWSourceDBPort;
string EDWSourceDBUserName;
string EDWSourceDBPassword;
string EDWSourceDB2;
string EDWSourceDBEngine2;
string EDWSourceDBServerName2;
string EDWSourceDBPort2;
string EDWSourceDBUserName2;
string EDWSourceDBPassword2;
string DMSTargetEngine;
string DMSTargetExtraAtt;
string DMSTargetRawExtraAtt;
string EDWSourceExtraAtt1;
string EDWSourceExtraAtt2;
string EDWSourceExtraAtt3;
string EDWSourceExtraAtt4;
string DMSCreateReplicationInstance;
string DMSCreateSourceEndpoint;
string DMSCreateDestinationEndpoint;
string DMSCreateRawDestinationEndpoint;
string DMSCreateTask;
string CreateProcessingZoneNotifications;
string EDWJobCompleteSNSArn;
string dmsReconciliationKMSKey;
string basicResourcesStackName;
string dmsResourcesStackName;
string jobsResourcesStackName;
string dropCreateSalesOrdersStackName;
string changeRequestNumber;
var isRunningInBamboo = Environment.GetEnvironmentVariables().Keys.Cast<string>().Any(k => k.StartsWith("bamboo_", StringComparison.OrdinalIgnoreCase));

//////////////////////////////////////////////////////////////////////
// Arguments
//////////////////////////////////////////////////////////////////////

// DO NOT assign from bamboo environment variables here. Do it in Task("Intialise")
var target = Argument("target", "Default");
string basicResourcesTemplatePath = "basicResourcesTemplate.json";
string dmsResourcesTemplatePath = "dmsResourcesTemplate.yaml";
string jobsResourcesTemplatePath = "jobsResourcesTemplate.json";
string dropCreateSalesOrdersTemplatePath = "dropCreateSalesOrdersTemplate.json";
// string securityGroupResourcesTemplatePath = "securityGroupResourcesTemplate.json";

// The following are assigned in Task("Intialise")
TagsInfo tagsInfo;
string receiverId;

//////////////////////////////////////////////////////////////////////
// PRE-DEPLOY
//////////////////////////////////////////////////////////////////////

Task("Initialise")
    .Does(() => {

        // Place any initialisation, e.g. AWS authentication or  setting of variables that require environment variables to exist.
        // Having this kind of initialisation in a task permits -WhatIf runs to check task run order.

        //////////////////////////////////////////////////////////////////////
        // Authenticate with AWS.
        // See
        // https://bitbucket.org/coreshowservices/reedexpo.cake.aws.buildauthentication
        //////////////////////////////////////////////////////////////////////
        awsCredentials = GetBuildAwsCredentials();

        // Deployment environment name (set by Bamboo)
        environment = EnvironmentVariableStrict("bamboo_deploy_environment");
        deploymentRole = EnvironmentVariableStrict("bamboo_AWS_DEPLOYMENT_ROLE_ARN");
//         PowerBIRDPSource = EnvironmentVariableStrict("bamboo_POWERBI_RDP_SOURCE");
//         EMRMasterSSHSourceIP = EnvironmentVariableStrict("bamboo_EMR_MASTER_SSH_SOURCE");
//         JumpBoxSG = EnvironmentVariableStrict("bamboo_JUMPBOX_SG");
        EDWSourceDB = EnvironmentVariableStrict("bamboo_EDW_SOURCE_DB");
        EDWSourceDBEngine = EnvironmentVariableStrict("bamboo_EDW_SOURCE_DB_ENGINE");
        EDWSourceDBServerName = EnvironmentVariableStrict("bamboo_EDW_SOURCE_DB_SERVER_NAME");
        EDWSourceDBPort = EnvironmentVariableStrict("bamboo_EDW_SOURCE_DB_PORT");
        EDWSourceDBUserName = EnvironmentVariableStrict("bamboo_EDW_SOURCE_DB_USER_NAME");
        EDWSourceDBPassword = EnvironmentVariableStrict("bamboo_EDW_SOURCE_DB_PASSWORD");
        EDWSourceDB2 = EnvironmentVariableStrict("bamboo_EDW_SOURCE_DB_2");
        EDWSourceDBEngine2 = EnvironmentVariableStrict("bamboo_EDW_SOURCE_DB_ENGINE_2");
        EDWSourceDBServerName2 = EnvironmentVariableStrict("bamboo_EDW_SOURCE_DB_SERVER_NAME_2");
        EDWSourceDBPort2 = EnvironmentVariableStrict("bamboo_EDW_SOURCE_DB_PORT_2");
        EDWSourceDBUserName2 = EnvironmentVariableStrict("bamboo_EDW_SOURCE_DB_USER_NAME_2");
        EDWSourceDBPassword2 = EnvironmentVariableStrict("bamboo_EDW_SOURCE_DB_PASSWORD_2");
        DMSTargetEngine = EnvironmentVariableStrict("bamboo_DMS_TARGET_ENGINE");
        DMSTargetExtraAtt = EnvironmentVariableStrict("bamboo_DMS_TARGET_EXTRA_ATT");
        DMSTargetRawExtraAtt = EnvironmentVariableStrict("bamboo_DMS_TARGET_RAW_EXTRA_ATT");
        EDWSourceExtraAtt1 = EnvironmentVariableStrict("bamboo_EDW_SOURCE_EXTRA_ATT_1");
        EDWSourceExtraAtt2 = EnvironmentVariableStrict("bamboo_EDW_SOURCE_EXTRA_ATT_2");
        EDWSourceExtraAtt3 = EnvironmentVariableStrict("bamboo_EDW_SOURCE_EXTRA_ATT_3");
        EDWSourceExtraAtt4 = EnvironmentVariableStrict("bamboo_EDW_SOURCE_EXTRA_ATT_4");
        DMSCreateReplicationInstance = EnvironmentVariableStrict("bamboo_DMS_CREATE_REP_INST");
        DMSCreateSourceEndpoint = EnvironmentVariableStrict("bamboo_DMS_CREATE_SOURCE_ENDPOINT");
        DMSCreateDestinationEndpoint = EnvironmentVariableStrict("bamboo_DMS_CREATE_TARGET_ENDPOINT");
        DMSCreateRawDestinationEndpoint = EnvironmentVariableStrict("bamboo_DMS_CREATE_RAW_TARGET_ENDPOINT");
        DMSCreateTask = EnvironmentVariableStrict("bamboo_DMS_CREATE_TASK");
        CreateProcessingZoneNotifications = EnvironmentVariableStrict("bamboo_CREATE_PROCESSING_ZONE_NOTIFICATIONS");
        EDWJobCompleteSNSArn = EnvironmentVariableStrict("bamboo_EDW_JOB_COMPLETE_SNS_ARN");
        receiverId = Argument<string>("receiverId", "");
        TagsInfoBuilder baseTagsBuilder = TagsInfoBuilder.Create()
            .WithEnvironmentName(environment)
            .WithFinanceEntityId("0092")
            .WithFinanceActivityId("8000")
            .WithFinanceManagementCentreId("99440")
            .WithPmProgramme("platform")
            .WithPmProjectCode("n/a")
            .WithJiraProjectCode("RDL")
            .WithServiceName("GBS Data Lake");
        TagsInfo baseTagsInfo = baseTagsBuilder;
        tagsInfo = baseTagsBuilder.WithEnvironmentName(environment);

        basicResourcesStackName = $"EMR-basic-resources-{environment}";
        dmsResourcesStackName = $"rx-gbs-datalake-dms-resources-{environment}";
        jobsResourcesStackName = $"EMR-jobs-resources-{environment}";
        dropCreateSalesOrdersStackName = "rx-gbs-datalake-drop-create-sales-orders";
//         securityGroupResourcesStackName = $"rx-gbs-datalake-sg-resources-{environment}";

        Information("Initialise task finished");

    });

Task("basicResourcesDeployment")
    .IsDependentOn("Initialise")
    .Description("Basic resources setup")
    .WithCriteria(environment != "local")
    .Does(() => {

        var result = RunCloudFormation(
            new RunCloudFormationSettings
            {
                Credentials = awsCredentials,
                Capabilities = new List<Capability> { Capability.CAPABILITY_NAMED_IAM, Capability.CAPABILITY_AUTO_EXPAND }, // Required for creation of named service user.
                Region = region,
                StackName = basicResourcesStackName,
                TemplatePath = basicResourcesTemplatePath,
                Parameters = new Dictionary<string, string>
                {
                    // TODO
                    // Add additional parameters e.g. database credentials gathered from plan variables here.
                    {"BillingEnvironmentName", environment},
                    {"deploymentRole", deploymentRole},
                    {"CreateProcessingZoneNotifications", CreateProcessingZoneNotifications}
                },
                Tags = tagsInfo         // Note that applying tags to the stack will cause all resources to have the same tags when created.
            }
        );

        Information($"Stack update result was {result}");

        codeBucketName = GetCloudFormationStackOutputs(awsCredentials, basicResourcesStackName)["ProcessingS3Bucket"];
        Information($"Code bucket name -> {codeBucketName}");
    });

Task("UploadCloudFormationTemplates")
    .IsDependentOn("basicResourcesDeployment")
    .Does(async () => {
        var templatesToUpload = new Dictionary<string, string>();
        var files = GetFiles("**/*.json");
        foreach(var file in files){
            templatesToUpload.Add(file.FullPath,"emr-jobs/cf-templates/"+file.GetFilename());
        }
        await upload_to_code_bucket(awsCredentials, region, codeBucketName, templatesToUpload);
    });

Task("UploadHandlersToCodeBucket")
    .IsDependentOn("UploadCloudFormationTemplates")
    .Does(async () => {
        var handlersToUpload = new Dictionary<string, string> {
            {"rx-gbs-datalake-check-emr-cluster-status.zip", "emr-jobs/lambdas/rx-gbs-datalake-check-emr-cluster-status.zip"},
            {"rx-gbs-datalake-create-emr-cluster.zip", "emr-jobs/lambdas/rx-gbs-datalake-create-emr-cluster.zip"},
            {"rx-gbs-datalake-dms-check-status.zip", "emr-jobs/lambdas/rx-gbs-datalake-dms-check-status.zip"},
            {"rx-gbs-datalake-dms-downgrade-replication-instance.zip", "emr-jobs/lambdas/rx-gbs-datalake-dms-downgrade-replication-instance.zip"},
            {"rx-gbs-datalake-dms-replication-instance-status.zip", "emr-jobs/lambdas/rx-gbs-datalake-dms-replication-instance-status.zip"},
            {"rx-gbs-datalake-dms-run-full-load-ps.zip", "emr-jobs/lambdas/rx-gbs-datalake-dms-run-full-load-ps.zip"},
            {"rx-gbs-datalake-dms-send-sns-from-step-function.zip", "emr-jobs/lambdas/rx-gbs-datalake-dms-send-sns-from-step-function.zip"},
            {"rx-gbs-datalake-dms-upgrade-replication-instance.zip", "emr-jobs/lambdas/rx-gbs-datalake-dms-upgrade-replication-instance.zip"},
            {"rx-gbs-datalake-etl-job-config-validation.zip", "emr-jobs/lambdas/rx-gbs-datalake-etl-job-config-validation.zip"},
            {"rx-gbs-datalake-get-emr-DNS.zip", "emr-jobs/lambdas/rx-gbs-datalake-get-emr-DNS.zip"},
            {"rx-gbs-datalake-get-emr-job-status-boto3.zip", "emr-jobs/lambdas/rx-gbs-datalake-get-emr-job-status-boto3.zip"},
            {"rx-gbs-datalake-send-sns-from-step-function.zip", "emr-jobs/lambdas/rx-gbs-datalake-send-sns-from-step-function.zip"},
            {"rx-gbs-datalake-submit-emr-job.zip", "emr-jobs/lambdas/rx-gbs-datalake-submit-emr-job.zip"},
            {"rx-gbs-datalake-submit-merge-emr-job.zip", "emr-jobs/lambdas/rx-gbs-datalake-submit-merge-emr-job.zip"},
            {"rx-gbs-datalake-terminate-emr-cluster.zip", "emr-jobs/lambdas/rx-gbs-datalake-terminate-emr-cluster.zip"}
        };
        await upload_to_code_bucket(awsCredentials, region, codeBucketName, handlersToUpload);
    });

Task("UploadConfigToCodeBucket")
    .IsDependentOn("UploadHandlersToCodeBucket")
    .Does(async () => {
        var configsToUpload = new Dictionary<string, string> {
            {"installpip3.sh", "config/drivers/installpip3.sh"}
        };
        var files = GetFiles("*.csv");
        foreach (var file in files) {
           configsToUpload.Add(file.FullPath,"config/etl_job_config_csv/"+file.GetFilename());
        }
        await upload_to_code_bucket(awsCredentials, region, codeBucketName, configsToUpload);
    });

Task("UploadDataSchemasToCodeBucket")
    .IsDependentOn("UploadConfigToCodeBucket")
    .Does(async () => {
        var templatesToUpload = new Dictionary<string, string>();
        var files = GetFiles("**/*_schema.json");
        foreach(var file in files){
            templatesToUpload.Add(file.FullPath,"emr-jobs/data_schemas/"+file.GetFilename());
        }
        await upload_to_code_bucket(awsCredentials, region, codeBucketName, templatesToUpload);
    });

Task("DeployToCodeBucket")
    .IsDependentOn("UploadDataSchemasToCodeBucket")
    .Does(async () => {

        // Upload code bucket artifacts

        var scriptsToDeploy = new Dictionary<string, string> {
            {"etl_job_modules.zip", "emr-jobs/jobs/etl_job_modules.zip"}
        };
        var files = GetFiles("*.py");
        foreach (var file in files) {
           scriptsToDeploy.Add(file.FullPath,"emr-jobs/jobs/"+file.GetFilename());
        }
        await upload_to_code_bucket(awsCredentials, region, codeBucketName, scriptsToDeploy);
    });

// Task("SecurityGroupResourcesDeployment")
//     .IsDependentOn("DeployToCodeBucket")
//     .Description("SG resources setup")
//     .WithCriteria(environment != "local")
//     .Does(() => {
//
//         var result = RunCloudFormation(
//             new RunCloudFormationSettings
//             {
//                 Credentials = awsCredentials,
//                 Capabilities = new List<Capability> { Capability.CAPABILITY_NAMED_IAM, Capability.CAPABILITY_AUTO_EXPAND }, // Required for creation of named service user.
//                 Region = region,
//                 StackName = securityGroupResourcesStackName,
//                 TemplatePath = securityGroupResourcesTemplatePath,
//                 Parameters = new Dictionary<string, string>
//                 {
//                     // TODO
//                     // Add additional parameters e.g. database credentials gathered from plan variables here.
//                     {"BillingEnvironmentName", environment},
//                     {"PowerBIRDPSource", PowerBIRDPSource},
//                     {"EMRMasterSSHSourceIP", EMRMasterSSHSourceIP},
//                     {"JumpBoxSG", JumpBoxSG}
//                 },
//                 Tags = tagsInfo         // Note that applying tags to the stack will cause all resources to have the same tags when created.
//             }
//         );
//
//         Information($"Stack update result was {result}");
//     });

Task("DMSResourcesDeployment")
    .IsDependentOn("DeployToCodeBucket")
    .Description("DMS resources setup")
    .WithCriteria(environment != "local")
    .Does(() => {

        var result = RunCloudFormation(
            new RunCloudFormationSettings
            {
                Credentials = awsCredentials,
                Capabilities = new List<Capability> { Capability.CAPABILITY_NAMED_IAM, Capability.CAPABILITY_AUTO_EXPAND }, // Required for creation of named service user.
                Region = region,
                StackName = dmsResourcesStackName,
                TemplatePath = dmsResourcesTemplatePath,
                Parameters = new Dictionary<string, string>
                {
                    // TODO
                    // Add additional parameters e.g. database credentials gathered from plan variables here.
                    {"BillingEnvironmentName", environment},
                    {"SourceDataBase", EDWSourceDB},
                    {"SourceDataBaseEngine", EDWSourceDBEngine},
                    {"SourceDataBaseServerName", EDWSourceDBServerName},
                    {"SourceDataBasePort", EDWSourceDBPort},
                    {"SourceDataBaseUserName", EDWSourceDBUserName},
                    {"SourceDataBasePassword", EDWSourceDBPassword},
                    {"SourceDataBase2", EDWSourceDB2},
                    {"SourceDataBaseEngine2", EDWSourceDBEngine2},
                    {"SourceDataBaseServerName2", EDWSourceDBServerName2},
                    {"SourceDataBasePort2", EDWSourceDBPort2},
                    {"SourceDataBaseUserName2", EDWSourceDBUserName2},
                    {"SourceDataBasePassword2", EDWSourceDBPassword2},
//                     {"SecurityGroups", GetCloudFormationStackOutputs(awsCredentials, securityGroupResourcesStackName)["DMSSGOutput"]},
                    {"TargetEngineNameForS3", DMSTargetEngine},
                    {"TargetS3BucketName", GetCloudFormationStackOutputs(awsCredentials, basicResourcesStackName)["LandingS3Bucket"]},
                    {"TargetS3RawBucketName", GetCloudFormationStackOutputs(awsCredentials, basicResourcesStackName)["RawS3Bucket"]},
                    {"TargetS3ExtraConnectionAttributes", DMSTargetExtraAtt},
                    {"TargetS3RawExtraConnectionAttributes", DMSTargetRawExtraAtt},
                    {"SourceExtraConnectionAttributes1", EDWSourceExtraAtt1},
                    {"SourceExtraConnectionAttributes2", EDWSourceExtraAtt2},
                    {"SourceExtraConnectionAttributes3", EDWSourceExtraAtt3},
                    {"SourceExtraConnectionAttributes4", EDWSourceExtraAtt4},
                    {"DmsKmsKey", GetCloudFormationStackOutputs(awsCredentials, basicResourcesStackName)["DMSKMSKeyOutput"]},
                    {"CreateReplicationInstanceOnly", DMSCreateReplicationInstance},
                    {"CreateSourceEndpointOnly", DMSCreateSourceEndpoint},
                    {"CreateDestinationEndpointOnly", DMSCreateDestinationEndpoint},
                    {"CreateRawDestinationEndpointOnly", DMSCreateRawDestinationEndpoint},
                    {"CreateTaskOnly", DMSCreateTask}
                },
                Tags = tagsInfo         // Note that applying tags to the stack will cause all resources to have the same tags when created.
            }
        );

        Information($"Stack update result was {result}");
    });

Task("jobsResourcesDeployment")
    .IsDependentOn("DMSResourcesDeployment")
    .Description("Jobs resources setup")
    .WithCriteria(environment != "local")
    .Does(() => {

        dmsReconciliationKMSKey = GetCloudFormationStackOutputs(awsCredentials, basicResourcesStackName)["DMSKMSKeyOutput"];

        var result = RunCloudFormation(
            new RunCloudFormationSettings
            {
                Credentials = awsCredentials,
                Capabilities = new List<Capability> { Capability.CAPABILITY_NAMED_IAM, Capability.CAPABILITY_AUTO_EXPAND }, // Required for creation of named service user.
                Region = region,
                StackName = jobsResourcesStackName,
                TemplatePath = jobsResourcesTemplatePath,
                Parameters = new Dictionary<string, string>
                {
                    // TODO
                    // Add additional parameters e.g. database credentials gathered from plan variables here.
                    {"BillingEnvironmentName", environment},
                    {"CodeBucketName", codeBucketName},
                    {"DmsReconciliationKMSKey", dmsReconciliationKMSKey},
                    {"ReceiverId", receiverId}
                },
                Tags = tagsInfo         // Note that applying tags to the stack will cause all resources to have the same tags when created.
            }
        );

        Information($"Stack update result was {result}");
    });

Task("Publish-Version")
    .IsDependentOn("jobsResourcesDeployment")
    .Description("Publishes a version of your function")
    .Does(() => {
    try
    {
        DeployLambda(new DeployLambdaSettings {
            Credentials = awsCredentials,
            DeploymentPackage = "rx-gbs-datalake-check-emr-cluster-status.zip",
            FunctionName = GetCloudFormationStackOutputs(awsCredentials, jobsResourcesStackName)["CheckEMRClusterStatusLambdaARN"]
        });
        DeployLambda(new DeployLambdaSettings {
            Credentials = awsCredentials,
            DeploymentPackage = "rx-gbs-datalake-create-emr-cluster.zip",
            FunctionName = GetCloudFormationStackOutputs(awsCredentials, jobsResourcesStackName)["CreateEMRClusterLambdaARN"]
        });
        DeployLambda(new DeployLambdaSettings {
            Credentials = awsCredentials,
            DeploymentPackage = "rx-gbs-datalake-dms-check-status.zip",
            FunctionName = GetCloudFormationStackOutputs(awsCredentials, jobsResourcesStackName)["DMSTaskStatusLambdaARN"]
        });
        DeployLambda(new DeployLambdaSettings {
            Credentials = awsCredentials,
            DeploymentPackage = "rx-gbs-datalake-dms-downgrade-replication-instance.zip",
            FunctionName = GetCloudFormationStackOutputs(awsCredentials, jobsResourcesStackName)["DMSDowngradeReplicationInstanceLambdaARN"]
        });
        DeployLambda(new DeployLambdaSettings {
            Credentials = awsCredentials,
            DeploymentPackage = "rx-gbs-datalake-dms-replication-instance-status.zip",
            FunctionName = GetCloudFormationStackOutputs(awsCredentials, jobsResourcesStackName)["DMSReplicationInstanceStatusLambdaARN"]
        });
        DeployLambda(new DeployLambdaSettings {
            Credentials = awsCredentials,
            DeploymentPackage = "rx-gbs-datalake-dms-run-full-load-ps.zip",
            FunctionName = GetCloudFormationStackOutputs(awsCredentials, jobsResourcesStackName)["DMSRunTaskLambdaARN"]
        });
        DeployLambda(new DeployLambdaSettings {
            Credentials = awsCredentials,
            DeploymentPackage = "rx-gbs-datalake-dms-send-sns-from-step-function.zip",
            FunctionName = GetCloudFormationStackOutputs(awsCredentials, jobsResourcesStackName)["DMSSendSNSFromStepFunctionLambdaARN"]
        });
        DeployLambda(new DeployLambdaSettings {
            Credentials = awsCredentials,
            DeploymentPackage = "rx-gbs-datalake-dms-upgrade-replication-instance.zip",
            FunctionName = GetCloudFormationStackOutputs(awsCredentials, jobsResourcesStackName)["DMSUpgradeReplicationInstanceLambdaARN"]
        });
        DeployLambda(new DeployLambdaSettings {
            Credentials = awsCredentials,
            DeploymentPackage = "rx-gbs-datalake-get-emr-DNS.zip",
            FunctionName = GetCloudFormationStackOutputs(awsCredentials, jobsResourcesStackName)["GetEMRDNSLambdaARN"]
        });
        DeployLambda(new DeployLambdaSettings {
            Credentials = awsCredentials,
            DeploymentPackage = "rx-gbs-datalake-get-emr-job-status-boto3.zip",
            FunctionName = GetCloudFormationStackOutputs(awsCredentials, jobsResourcesStackName)["GetEMRJobStatusLambdaARN"]
        });
        DeployLambda(new DeployLambdaSettings {
            Credentials = awsCredentials,
            DeploymentPackage = "rx-gbs-datalake-send-sns-from-step-function.zip",
            FunctionName = GetCloudFormationStackOutputs(awsCredentials, jobsResourcesStackName)["SendSNSFromStepFunctionLambdaARN"]
        });
        DeployLambda(new DeployLambdaSettings {
            Credentials = awsCredentials,
            DeploymentPackage = "rx-gbs-datalake-submit-emr-job.zip",
            FunctionName = GetCloudFormationStackOutputs(awsCredentials, jobsResourcesStackName)["SubmitEMRJobLambdaARN"]
        });
        DeployLambda(new DeployLambdaSettings {
            Credentials = awsCredentials,
            DeploymentPackage = "rx-gbs-datalake-submit-merge-emr-job.zip",
            FunctionName = GetCloudFormationStackOutputs(awsCredentials, jobsResourcesStackName)["SubmitEMRMergeJobLambdaARN"]
        });
        DeployLambda(new DeployLambdaSettings {
            Credentials = awsCredentials,
            DeploymentPackage = "rx-gbs-datalake-terminate-emr-cluster.zip",
            FunctionName = GetCloudFormationStackOutputs(awsCredentials, jobsResourcesStackName)["TerminateEMRClusterLambdaARN"]
        });
    }
    catch(Exception exception)
    {
      Information(exception.ToString());
    }
});

// Task("dropCreateSalesOrdersResourcesDeployment")
//     .WithCriteria(environment != "dev")
//     .IsDependentOn("Publish-Version")
//     .Description("DropCreate Sales Orders resources setup")
//     .Does(() => {
//
//         var result = RunCloudFormation(
//             new RunCloudFormationSettings
//             {
//                 Credentials = awsCredentials,
//                 Capabilities = new List<Capability> { Capability.CAPABILITY_NAMED_IAM, Capability.CAPABILITY_AUTO_EXPAND }, // Required for creation of named service user.
//                 Region = region,
//                 StackName = dropCreateSalesOrdersStackName,
//                 TemplatePath = dropCreateSalesOrdersTemplatePath,
//                 Parameters = new Dictionary<string, string>
//                 {
//                     // TODO
//                     // Add additional parameters e.g. database credentials gathered from plan variables here.
//                     {"BillingEnvironmentName", environment},
//                     {"EDWJobCompleteSNSArn", EDWJobCompleteSNSArn},
//                     {"LogsBucketName", GetCloudFormationStackOutputs(awsCredentials, basicResourcesStackName)["LogsS3Bucket"]},
//                     {"LogsBucketKMSKeyArn", GetCloudFormationStackOutputs(awsCredentials, basicResourcesStackName)["LogsKMSKeyOutput"]},
//                     {"LambdaServiceRoleArn", GetCloudFormationStackOutputs(awsCredentials, jobsResourcesStackName)["lambdaServiceRoleARN"]},
//                     {"CodeBucketName", codeBucketName}
//                 },
//                 Tags = tagsInfo         // Note that applying tags to the stack will cause all resources to have the same tags when created.
//             }
//         );
//
//         Information($"Stack update result was {result}");
//     });


//////////////////////////////////////////////////////////////////////
// POST DEPLOYMENT
//////////////////////////////////////////////////////////////////////

Teardown(context => {

        // Teardown is EXECUTED by -WhatIf. Only permit execution when running in Bamboo
        if (isRunningInBamboo && !string.IsNullOrWhiteSpace(changeRequestNumber))
        {
            Information("Inside teardown");
            CloseChangeRequest(new ChangeRequestSettings
            {
                SystemId = changeRequestNumber,
                Success  = context.Successful,
            });
        }
 });

Task("Default")
    .IsDependentOn("Publish-Version");

RunTarget(target);

//////////////////////////////////////////////////////////////////////
// HELPER FUNCTIONS/CLASSES
//////////////////////////////////////////////////////////////////////

string EnvironmentVariableStrict(string key)
{
    var value = EnvironmentVariable(key);

    if (value == null)
    {
        throw new Exception("Environment Variable not found: " + key);
    }

    return value;
}

string EnvironmentVariableOrDefault(string key, string defaultValue)
{
    var value = EnvironmentVariable(key);
    return value ?? defaultValue;
}

async Task upload_to_code_bucket(AWSCredentials awsCredentials, Amazon.RegionEndpoint region, string codeBucketName,
                          Dictionary<string, string> filesToUpload){
    var immutableCredentials = awsCredentials.GetCredentials();

    // https://github.com/SharpeRAD/Cake.AWS.S3
    var uploadSettings = new UploadSettings {
        AccessKey = immutableCredentials.AccessKey,
        SecretKey = immutableCredentials.SecretKey,
        SessionToken = immutableCredentials.Token,
        Region = region,
        BucketName = codeBucketName
    };

    foreach (var kv in filesToUpload)
    {
        Information($"Uploading {kv.Key} to s3://{codeBucketName}/{kv.Value}");

        await S3Upload(kv.Key, kv.Value, uploadSettings);
    }
}
