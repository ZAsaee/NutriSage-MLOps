from aws_cdk import (
    Stack,
    Duration,
    Fn,
    aws_iam as iam,
    aws_s3 as s3,
    aws_stepfunctions as sfn,
    aws_stepfunctions_tasks as tasks,
)
from constructs import Construct


class PipelineStack(Stack):
    """Step‑Functions pipeline that
    1. Runs a SageMaker Processing job to clean raw data (sync integration)
    2. Launches a Glue crawler to catalogue the processed partition
    """

    def __init__(self, scope: Construct, construct_id: str, *, env=None, **kwargs):
        super().__init__(scope, construct_id, **kwargs)

        # ────────────────────────────────────────
        #  Cross‑stack imports
        # ────────────────────────────────────────
        raw_bucket_name = Fn.import_value("NutriSageDataLake-raw-bucket")
        processed_bucket_name = Fn.import_value(
            "NutriSageDataLake-processed-bucket")
        crawler_name = Fn.import_value("NutriSageProcessedCrawler")

        raw_bucket = s3.Bucket.from_bucket_name(
            self, "RawBucket", raw_bucket_name)
        processed_bucket = s3.Bucket.from_bucket_name(
            self, "ProcessedBucket", processed_bucket_name)

        # ────────────────────────────────────────
        #  IAM role for the processing job
        # ────────────────────────────────────────
        sm_role = iam.Role(
            self,
            "SageMakerProcessingRole",
            assumed_by=iam.ServicePrincipal("sagemaker.amazonaws.com"),
        )
        raw_bucket.grant_read(sm_role)
        processed_bucket.grant_read_write(sm_role)

        # ────────────────────────────────────────
        #  Step 1 – SageMaker Processing (sync)
        # ────────────────────────────────────────
        processing_job_state_json = {
            "Type": "Task",
            "Resource": "arn:aws:states:::sagemaker:createProcessingJob.sync",
            "Parameters": {
                "ProcessingJobName.$": "States.Format('clean-{}', States.UUID())",
                "RoleArn": sm_role.role_arn,
                # TODO: replace with your own container image
                "AppSpecification": {
                    "ImageUri": "763104351884.dkr.ecr.us-east-1.amazonaws.com/pytorch-inference:2.0.0-cpu-py310-ubuntu20.04",
                },
                "ProcessingResources": {
                    "ClusterConfig": {
                        "InstanceCount": 1,
                        "InstanceType": "ml.m5.xlarge",
                        "VolumeSizeInGB": 30,
                    }
                },
                "ProcessingInputs": [
                    {
                        "InputName": "raw",
                        "S3Input": {
                            "S3Uri": f"s3://{raw_bucket_name}/raw/",
                            "LocalPath": "/opt/ml/processing/input",
                            "S3DataType": "S3Prefix",
                            "S3InputMode": "File",
                        },
                    }
                ],
                "ProcessingOutputConfig": {
                    "Outputs": [
                        {
                            "OutputName": "clean",
                            "S3Output": {
                                "S3Uri": f"s3://{processed_bucket_name}/clean/",
                                "LocalPath": "/opt/ml/processing/output",
                                "S3UploadMode": "EndOfJob",
                            },
                        }
                    ]
                },
            },
        }

        step_clean = sfn.CustomState(
            self, "CleanRawData", state_json=processing_job_state_json)

        # ────────────────────────────────────────
        #  Step 2 – Start Glue crawler (fire‑and‑forget)
        # ────────────────────────────────────────
        step_crawl = tasks.CallAwsService(
            self,
            "StartGlueCrawler",
            service="glue",
            action="startCrawler",
            parameters={"Name": crawler_name},
            iam_resources=["*"],  # tighten later if you prefer
        )

        # ────────────────────────────────────────
        #  Assemble state machine
        # ────────────────────────────────────────
        definition = step_clean.next(step_crawl)

        state_machine = sfn.StateMachine(
            self,
            "NutriSagePipeline",
            definition=definition,
            timeout=Duration.hours(2),
        )

        # Allow the state machine to:
        # • create / manage the EventBridge rule used by the .sync pattern
        # • start & monitor the SageMaker Processing job
        # • pass the processing-job role to SageMaker
        state_machine.role.add_to_policy(
            iam.PolicyStatement(
                actions=[
                    # EventBridge managed rule for .sync
                    "events:PutRule",
                    "events:PutTargets",
                    "events:DescribeRule",
                    "events:DeleteRule",
                    # SageMaker job lifecycle
                    "sagemaker:CreateProcessingJob",
                    "sagemaker:DescribeProcessingJob",
                    "sagemaker:StopProcessingJob",
                    # Allow SFN to pass the runtime role to SageMaker
                    "iam:PassRole",
                ],
                # least‑privilege scoping possible once tested
                resources=["*"],
            )
        )
