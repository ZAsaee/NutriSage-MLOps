# lib/pipeline_stack.py
from aws_cdk import (
    Stack,
    Fn,
    Duration,
    aws_ec2 as ec2,
    aws_ecs as ecs,
    aws_iam as iam,
    aws_glue as glue,
    aws_stepfunctions as sfn,
    aws_stepfunctions_tasks as tasks,
)
from constructs import Construct


class PipelineStack(Stack):
    def __init__(self, scope: Construct, id: str, **kwargs):
        super().__init__(scope, id, **kwargs)

        db_name = Fn.import_value("NutrisageDB")

        processed_bucket = Fn.import_value(
            "NutriSageDataLake-processed-bucket")
        crawler_role = iam.Role(
            self, "CrawlerRole",
            assumed_by=iam.ServicePrincipal("glue.amazonaws.com"),
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name(
                    "service-role/AWSGlueServiceRole")
            ]
        )
        crawler_role.add_to_policy(
            iam.PolicyStatement(
                actions=["s3:GetObject", "s3:ListBucket"],
                resources=[
                    f"arn:aws:s3:::{processed_bucket}",
                    f"arn:aws:s3:::{processed_bucket}/processed/*"
                ]
            )
        )

        crawler = glue.CfnCrawler(
            self, "ProcessedCrawler",
            name="nutrisage-processed-crawler",
            role=crawler_role.role_arn,
            database_name=db_name,
            targets=glue.CfnCrawler.TargetsProperty(
                s3_targets=[glue.CfnCrawler.S3TargetProperty(
                    path=f"s3://{processed_bucket}/processed/"
                )]
            )
        )

        # sagemaker role
        sagemaker_role = iam.Role.from_role_arn(
            self, "ExecRole",
            role_arn="arn:aws:iam::<acct>:role/service-role/AmazonSageMaker-ExecutionRole-20250629T141707",
            mutable=True
        )
