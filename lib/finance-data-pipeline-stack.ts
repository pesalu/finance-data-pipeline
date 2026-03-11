import * as cdk from "aws-cdk-lib";
import { Construct } from "constructs";
import * as lambda from "aws-cdk-lib/aws-lambda";
import * as s3 from "aws-cdk-lib/aws-s3";
import * as ssm from "aws-cdk-lib/aws-ssm";
import * as secretsmanager from "aws-cdk-lib/aws-secretsmanager";
import * as stepfunctions from "aws-cdk-lib/aws-stepfunctions";
import * as tasks from "aws-cdk-lib/aws-stepfunctions-tasks";

export class FinanceIngestionStack extends cdk.Stack {
  constructor(scope: Construct, id: string, props?: cdk.StackProps) {
    super(scope, id, props);

    const bucket = new s3.Bucket(this, "FinanceDataBucket", {
      blockPublicAccess: s3.BlockPublicAccess.BLOCK_ALL,
    });

    const baseUrl1 = "https://financialmodelingprep.com/api/v3"
    const baseUrl2 = "https://financialmodelingprep.com/stable"
    const apiBaseUrl = new ssm.StringParameter(this, "FinanceApiBaseUrl", {
      parameterName: "/finance/api/baseUrl",
      stringValue: baseUrl2,
    });

    const financeApiSecret = new secretsmanager.Secret(this, "FinanceApiSecret", {
      secretName: "FinanceApiCredentials",
      generateSecretString: {
        secretStringTemplate: JSON.stringify({
          baseUrl: baseUrl2
        }),
        generateStringKey: "apiKey"
      }
    });

    // Shared bundling config for all Python Lambdas
    const pythonBundling = {
      image: lambda.Runtime.PYTHON_3_12.bundlingImage,
      command: [
        "bash", "-c",
        [
          "pip install -r requirements.txt -t /asset-output",
          "cp -au . /asset-output"
        ].join(" && ")
      ]
    };

    const getChunkList = new lambda.Function(this, "GetChunkList", {
      runtime: lambda.Runtime.PYTHON_3_12,
      handler: "index.lambda_handler",
      code: lambda.Code.fromAsset("lambda/getChunkList", {
        bundling: pythonBundling,
      }),
      timeout: cdk.Duration.seconds(30),
      environment: {
        BASE_URL_PARAM: apiBaseUrl.parameterName,
        FINANCE_SECRET: financeApiSecret.secretArn,
      },
    });

    const downloadChunk = new lambda.Function(this, "DownloadChunk", {
      runtime: lambda.Runtime.PYTHON_3_12,
      handler: "index.lambda_handler",
      code: lambda.Code.fromAsset("lambda/downloadChunk", {
        bundling: pythonBundling,
      }),
      timeout: cdk.Duration.seconds(30),
      environment: {
        BUCKET: bucket.bucketName,
        BASE_URL_PARAM: apiBaseUrl.parameterName,
        FINANCE_SECRET: financeApiSecret.secretArn,
      },
    });

    const finalizeJob = new lambda.Function(this, "FinalizeJob", {
      runtime: lambda.Runtime.PYTHON_3_12,
      handler: "index.lambda_handler",
      code: lambda.Code.fromAsset("lambda/finalizeJob", {
        bundling: pythonBundling,
      }),
      timeout: cdk.Duration.seconds(30),
      environment: {
        BUCKET: bucket.bucketName
      },
    });

    bucket.grantWrite(downloadChunk);
    bucket.grantWrite(finalizeJob)
    apiBaseUrl.grantRead(getChunkList);
    apiBaseUrl.grantRead(downloadChunk);
    financeApiSecret.grantRead(getChunkList);
    financeApiSecret.grantRead(downloadChunk);

    const getChunksTask = new tasks.LambdaInvoke(this, "GetChunks", {
      lambdaFunction: getChunkList,
      outputPath: "$.Payload",
    });

    const downloadChunkTask = new tasks.LambdaInvoke(this, "DownloadChunkTask", {
      lambdaFunction: downloadChunk,
      outputPath: "$.Payload",
    });

    const finalizeTask = new tasks.LambdaInvoke(this, "Finalize", {
      inputPath: "$",
      lambdaFunction: finalizeJob,
      outputPath: "$.Payload",
    });

    const mapState = new stepfunctions.Map(this, "ProcessChunks", {
      itemsPath: "$.windows",
      resultPath: "$.results",
      maxConcurrency: 20
    });

    mapState.itemProcessor(downloadChunkTask);

    const definition = getChunksTask.next(mapState).next(finalizeTask);

    new stepfunctions.StateMachine(this, "FinanceIngestionStateMachine", {
      definition,
      timeout: cdk.Duration.hours(1),
    });
  }
}
