***Orchestrating Transient Data Analytics Workflows via AWS Step Functions***

## Introduction

***AWS Step Functions*** is a fully managed service designed to coordinate and chain a series of steps together to create something called a state machine for automation tasks.  It supports visual workflows and state machines are defined as JSON structure via ***Amazon State Language*** (*ASL*).  In addition, state machines can be scheduled via ***Amazon CloudWatch*** as an event rule cron expression.  

In this blog, I will walk you through 1.) how to orchestrate data processing jobs via ***Amazon EMR*** and 2.) how to apply batch transform on a trained machine learning model to write predictions via ***Amazon SageMaker***.  *Step Functions* can be integrated with a wide variety of ***AWS*** services including: *AWS Lambda*, *AWS Fargate*, *AWS Batch*, *AWS Glue*, *Amazon ECS*, *Amazon SQS*, *Amazon SNS*, *Amazon DynamoDB*, and more.

## Example 1: Orchestrate Data Processing Jobs via Amazon EMR

1a.) Let's view our input sample dataset (dummy data from my favorite video game) in *Amazon S3*.

![1a-destiny-dataset.png](../master/blogs/images/1a-destiny-dataset.png)

1b.) Next, I will create a state machine that spins up an *EMR* cluster (group of *EC2* instances) via *ASL*.

```json
"Create_Infra": {
      "Type": "Task",
      "Resource": "arn:<partition>:states:<region>:<account-id>:elasticmapreduce:createCluster.sync",
      "Parameters": {
        "Name": "Demo",
        "VisibleToAllUsers": true,
        "ReleaseLabel": "emr-5.29.0",
        "Applications": [
          {
            "Name": "Hadoop"
          },
          {
            "Name": "Spark"
          },
          {
            "Name": "Hive"
          },
          {
            "Name": "Sqoop"
          }
        ],
        "ServiceRole": "EMR_DefaultRole",
        "JobFlowRole": "EMR_EC2_DefaultRole",
        "LogUri": "s3://aws-logs-<account-id>-<region>/elasticmapreduce/",
        "Instances": {
          "KeepJobFlowAliveWhenNoSteps": true,
          "InstanceGroups": [
            {
              "Name": "Master Instance Group",
              "InstanceRole": "MASTER",
              "InstanceCount": 1,
              "InstanceType": "m5.xlarge",
              "Market": "ON_DEMAND"
            },
            {
              "Name": "Core Instance Group",
              "InstanceRole": "CORE",
              "InstanceCount": 1,
              "InstanceType": "m5.xlarge",
              "Market": "ON_DEMAND"
            },
            {
              "Name": "Task Instance Group",
              "InstanceRole": "TASK",
              "InstanceCount": 2,
              "InstanceType": "m5.xlarge",
              "Market": "ON_DEMAND"
            }
          ],
          "Ec2KeyName": "<ec2-key>",
          "Ec2SubnetId": "<subnet>",
          "EmrManagedMasterSecurityGroup": "<security-group>",
          "EmrManagedSlaveSecurityGroup": "<security-group>",
          "ServiceAccessSecurityGroup": "<security-group>"
        }
      },
      "ResultPath": "$.cluster",
      "Next": "Example_Job_Step_1"
    }
```

1c.) Now we can perform some data processing (simple partition by a column) by submitting a job to the cluster and terminating infrastructure upon completion via *ASL*.  Letís also inspect our output data in *S3*.

```json
"Example_Job_Step_1": {
      "Type": "Task",
      "Resource": "arn:<partition>:states:<region>:<account-id>:elasticmapreduce:addStep.sync",
      "Parameters": {
        "ClusterId.$": "$.cluster.ClusterId",
        "Step": {
          "Name": "aws-step-functions-example",
          "ActionOnFailure": "TERMINATE_CLUSTER",
          "HadoopJarStep": {
            "Jar": "command-runner.jar",
            "Args": [
              "spark-submit",
              "--deploy-mode",
              "cluster",
              "--class",
              "awsStepFunctionsExampleApp",
              "s3://<bucket>/datasets/user/demos/aws-step-functions-example/jars/aws-step-functions-demo_2.11-1.0.jar",
              "s3://<bucket>/datasets/user/demos/aws-step-functions-example/input.csv",
              "class",
              "s3://<bucket>/datasets/user/demos/aws-step-functions-example/output.parquet"
            ]
          }
        }
      },
      "ResultPath": "$.firstStep",
      "Next": "Terminate_Infra"
    }
```

```json
    "Terminate_Infra": {
      "Type": "Task",
      "Resource": "arn:<partition>:states:<region>:<account-id>:elasticmapreduce:terminateCluster.sync",
      "Parameters": {
        "ClusterId.$": "$.cluster.ClusterId"
      },
      "End": true
    }
```

![1c-destiny-partitions.png](/api/uploads/f2d2b755f40c0a8416fc9366b89392e5/1c-destiny-partitions.png)

1d.) Here is the complete JSON *ASL* structure and a visual workflow screenshot of what is going on.

```json
{
  "Comment": "Execute Amazon EMR Job Example",
  "StartAt": "Create_Infra",
  "States": {
    "Create_Infra": {
      "Type": "Task",
      "Resource": "arn:<partition>:states:<region>:<account-id>:elasticmapreduce:createCluster.sync",
      "Parameters": {
        "Name": "Demo",
        "VisibleToAllUsers": true,
        "ReleaseLabel": "emr-5.29.0",
        "Applications": [
          {
            "Name": "Hadoop"
          },
          {
            "Name": "Spark"
          },
          {
            "Name": "Hive"
          },
          {
            "Name": "Sqoop"
          }
        ],
        "ServiceRole": "EMR_DefaultRole",
        "JobFlowRole": "EMR_EC2_DefaultRole",
        "LogUri": "s3://aws-logs-<account-id>-<region>/elasticmapreduce/",
        "Instances": {
          "KeepJobFlowAliveWhenNoSteps": true,
          "InstanceGroups": [
            {
              "Name": "Master Instance Group",
              "InstanceRole": "MASTER",
              "InstanceCount": 1,
              "InstanceType": "m5.xlarge",
              "Market": "ON_DEMAND"
            },
            {
              "Name": "Core Instance Group",
              "InstanceRole": "CORE",
              "InstanceCount": 1,
              "InstanceType": "m5.xlarge",
              "Market": "ON_DEMAND"
            },
            {
              "Name": "Task Instance Group",
              "InstanceRole": "TASK",
              "InstanceCount": 2,
              "InstanceType": "m5.xlarge",
              "Market": "ON_DEMAND"
            }
          ],
          "Ec2KeyName": "<ec2-key>",
          "Ec2SubnetId": "<subnet>",
          "EmrManagedMasterSecurityGroup": "<security-group>",
          "EmrManagedSlaveSecurityGroup": "<security-group>",
          "ServiceAccessSecurityGroup": "<security-group>"
        }
      },
      "ResultPath": "$.cluster",
      "Next": "Example_Job_Step_1"
    },
    "Example_Job_Step_1": {
      "Type": "Task",
      "Resource": "arn:<partition>:states:<region>:<account-id>:elasticmapreduce:addStep.sync",
      "Parameters": {
        "ClusterId.$": "$.cluster.ClusterId",
        "Step": {
          "Name": "aws-step-functions-example",
          "ActionOnFailure": "TERMINATE_CLUSTER",
          "HadoopJarStep": {
            "Jar": "command-runner.jar",
            "Args": [
              "spark-submit",
              "--deploy-mode",
              "cluster",
              "--class",
              "awsStepFunctionsExampleApp",
              "s3://<bucket>/datasets/user/demos/aws-step-functions-example/jars/aws-step-functions-demo_2.11-1.0.jar",
              "s3://<bucket>/datasets/user/demos/aws-step-functions-example/input.csv",
              "class",
              "s3://<bucket>/datasets/user/demos/aws-step-functions-example/output.parquet"
            ]
          }
        }
      },
      "ResultPath": "$.firstStep",
      "Next": "Terminate_Infra"
    },
    "Terminate_Infra": {
      "Type": "Task",
      "Resource": "arn:<partition>:states:<region>:<account-id>:elasticmapreduce:terminateCluster.sync",
      "Parameters": {
        "ClusterId.$": "$.cluster.ClusterId"
      },
      "End": true
    }
  }
}
```

![1d-emr-visual-workflow.png](/api/uploads/c8d9451f6f8b636521ab9607abb851f6/1d-emr-visual-workflow.png)

1e.) Finally, letís schedule via *CloudWatch* to execute every 15 min as a simple cron expression (0 */15 * ? * *).

![1e-cloudwatch-cron.png](/api/uploads/7373244ba8446b270504f30fb234742f/1e-cloudwatch-cron.png)

In summary for this example, you can utilize *Step Functions* to automate and schedule your data processing jobs.  The level of sophistication typically depends on data volume and can range from a few simple steps on a single *EC2* machine to distributing multiple jobs in parallel on the same cluster or across multiple clusters with different instance types.  I recommend including additional *EMR* tuning configurations for selected software (i.e. YARN, Spark, Hive, Sqoop) in the JSON *ASL* to optimize job performance.  Also, choose the number and kind of *EC2* instance types wisely to save costs and execution time.  Your decision should mostly depend on the total data volume that needs processed and job type (CPU or memory constrained).

To the next example Ö

## Example 2: Apply Batch Transform on Data for Inferencing with a Trained ML Model via Amazon SageMaker

2a.) For data understanding letís view the raw labeled (dependent variable is the last column named *rings*) training dataset (abalone from the *UCI Machine Learning Repository*) in *S3*.  The trained model predicts age of the abalone (a type of shellfish) from physical measurements. 

https://archive.ics.uci.edu/ml/datasets/abalone

![2a-abalone-dataset.png](/api/uploads/8c6b3bcaded348ceaab593f8e8c049c8/2a-abalone-dataset.png)

2b.) Next, letís create the *ASL* structure triggering a batch transform job on a raw unlabeled (no *rings* column) batch dataset that needs inference via the trained model stored in *S3*.  ***Please note currently you must attach an inline policy to the role selected for the state machine***.

![2b-inline-policy.png](/api/uploads/43caec675e1f8af89759469b58fcf3ed/2b-inline-policy.png)

![2b-attach-inline-policy.png](/api/uploads/9c99dd104d64cb0193441b72b56e1362/2b-attach-inline-policy.png)

```json
{
  "StartAt": "Batch_Transform_Inference_Job",
  "States": {
    "Batch_Transform_Inference_Job": {
      "Type": "Task",
      "Resource": " arn:<partition>:states:<region>:<account-id>:sagemaker:createTransformJob.sync",
      "Parameters": {
        "ModelName": "inference-pipeline-example-model",
        "TransformInput": {
          "CompressionType": "None",
          "ContentType": "text/csv",
          "DataSource": {
            "S3DataSource": {
              "S3DataType": "S3Prefix",
              "S3Uri": "s3://<bucket>/datasets/user/demos/aws-step-functions-example/abalone/batch/batch.csv"
            }
          }
        },
        "TransformOutput": {
          "S3OutputPath": "s3://<bucket>/datasets/user/demos/aws-step-functions-example/abalone/inference/predictions.csv"
        },
        "TransformResources": {
          "InstanceCount": 1,
          "InstanceType": "ml.m5.xlarge"
        },
        "TransformJobName": "sagemaker-batch-transform-example-job-12345678910"
      },
      "End": true
    }
  }
}
```

2c.) Lastly, we can view the *Step Functions* visual workflow and the jobís output results with a prediction score column added.  Note, the model is a pipeline model that includes preprocessing  (one hot encoding, scaling, etc.) the data before sending it to the supervised learning algorithm.

![2c-sagemaker-visual-workflow.png](/api/uploads/d55c69d8a33bb4586092ea54ad3d2b5c/2c-sagemaker-visual-workflow.png)

![2c-batch-transform-output.png](/api/uploads/e0268e4203f52f2849a57f3829cc36f3/2c-batch-transform-output.png)

In summary for this example, you can utilize *Step Functions* to automate and schedule your machine learning jobs (pre-processing, training, tuning, model hosting, self-service & batch inference).  *Step Functions* and *SageMaker* integration support the following APIs: ```CreateEndpoint```, ```CreateEndpointConfig```, ```CreateHyperParameterTuningJob```, ```CreateLabelingJob```, ```CreateModel```, ```CreateTrainingJob```, and ```CreateTransformJob```.

## Conclusion

These two examples cover a small portion of what ***AWS*** services are capable of to scale data engineering and machine learning workflows.  Thank you for reading this blog. Please reach out with any questions, interests, collaboration, and or feedback.
