from aws_cdk import (
    Stack, RemovalPolicy, aws_s3 as s3, CfnOutput
)
from constructs import Construct
import os


class DataLakeStack(Stack):
    def __init__(self, scope: Construct, cid: str, **kwargs) -> None:
        super().__init__(scope, cid, **kwargs)

        acct = os.getenv("CDK_DEFAULT_ACCOUNT")
        prefix = os.getenv("PROJECT_PREFIX", "nutrisage")

        self.raw_bucket = s3.Bucket(
            self, "RawBucket",
            bucket_name=f"{prefix}-raw-{acct}",
            encryption=s3.BucketEncryption.S3_MANAGED,
            block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
            removal_policy=RemovalPolicy.DESTROY
        )

        self.processed_bucket = s3.Bucket(
            self, "ProcessedBucket",
            bucket_name=f"{prefix}-processed-{acct}",
            encryption=s3.BucketEncryption.S3_MANAGED,
            block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
            removal_policy=RemovalPolicy.DESTROY
        )

        CfnOutput(self, "RawBucketName", value=self.raw_bucket.bucket_name)
        CfnOutput(self, "ProcessedBucketName",
                  value=self.processed_bucket.bucket_name)
