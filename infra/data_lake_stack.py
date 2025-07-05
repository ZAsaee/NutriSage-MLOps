from pathlib import Path

from aws_cdk import (
    Stack,
    aws_s3 as s3,
    aws_glue as glue,
    aws_iam as iam,
    RemovalPolicy,
    Duration,
    CfnOutput,
    aws_s3_deployment as s3deploy,
    aws_logs as logs,
)
from constructs import Construct
import yaml
import os

# Helper: load column list to build the the Glue schema

COLS_YAML = Path(__file__).parents[1] / "src" / "fe" / "candidate-columns.yml"
KEEP_COLS = yaml.safe_load(COLS_YAML.read_text())["columns"]
TARGET = "nutrition_grade_fr"


class DataLakeStack(Stack):
    def __init__(self, scope: Construct, cid: str, **kwargs) -> None:
        super().__init__(scope, cid, **kwargs)

        account = os.getenv("CDK_DEFAULT_ACCOUNT")
        prefix = os.getenv("PROJECT_PREFIX", "Nutrisage")

        # s3 bucket for raw data
        self.raw_bucket = s3.Bucket.from_bucket_name(
            self, "RawBucketImported",
            bucket_name=f"{prefix}-raw-{account}"
        )

        # s3 bucket for processed data
        self.processed_bucket = s3.Bucket.from_bucket_name(
            self, "ProcessedBucketImported",
            bucket_name=f"{prefix}-processed-{account}"
        )

        # s3 bucket for athena results
        self.athena_results_bucket = s3.Bucket.from_bucket_name(
            self,
            "AthenaResultsBucketImported",
            bucket_name=f"{prefix}-athena-results-{account}"
        )

        # Glue database
        self.database = glue.CfnDatabase(
            self, "DB",
            catalog_id=self.account,
            database_input=glue.CfnDatabase.DatabaseInputProperty(
                name=f"{prefix}_datalake"),
        )

        # Create crawler and IAM role
        glue_role = iam.Role(
            self, "GlueCrawlerRole",
            assumed_by=iam.ServicePrincipal("glue.amazonaws.com"),
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name(
                    "service-role/AWSGlueServiceRole")
            ]
        )

        self.processed_bucket.grant_read(
            glue_role)   # allow crawler to scan data

        processed_crawler = glue.CfnCrawler(
            self, "ProcessedCrawler",
            name="nutrisage-processed-crawler",
            role=glue_role.role_arn,
            database_name=self.database.ref,                # existing Glue DB
            targets=glue.CfnCrawler.TargetsProperty(
                s3_targets=[glue.CfnCrawler.S3TargetProperty(
                    # partition root
                    path=f"s3://{self.processed_bucket.bucket_name}/processed/"
                )]
            ),
            table_prefix="foodfacts_clean_",
            schema_change_policy=glue.CfnCrawler.SchemaChangePolicyProperty(
                update_behavior="LOG",
                delete_behavior="DEPRECATE_IN_DATABASE"
            )
        )

        # Glue table using the same processed bucket
        # table=glue.CfnTable(
        #     self, "Table",
        #     catalog_id=self.account,
        #     database_name=self.database.ref,
        #     table_input=glue.CfnTable.TableInputProperty(
        #         name="foodfacts_raw",
        #         table_type="EXTERNAL_TABLE",
        #         partition_keys=[
        #             glue.CfnTable.ColumnProperty(name="year", type="int"),
        #             glue.CfnTable.ColumnProperty(
        #                 name="country", type="string"),
        #         ],
        #         storage_descriptor=glue.CfnTable.StorageDescriptorProperty(
        #             location=f"s3://{self.processed_bucket.bucket_name}/processed/",
        #             input_format="org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat",
        #             output_format="org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat",
        #             serde_info=glue.CfnTable.SerdeInfoProperty(
        #                 serialization_library="org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe",
        #             ),
        #             columns=[
        #                 glue.CfnTable.ColumnProperty(name=c, type="string") for c in KEEP_COLS
        #             ],
        #         ),
        #     ),
        # )
        # table.add_dependency(self.database)

        # Export raw bucket name for downstream stacks
        CfnOutput(
            self, "RawBucketName",
            value=self.raw_bucket.bucket_name,
            export_name=f"{self.stack_name}-raw-bucket"
        )

        # Export processed bucket name for downstream stacks
        CfnOutput(
            self, "ProcessedBucketName",
            value=self.processed_bucket.bucket_name,
            export_name=f"{self.stack_name}-processed-bucket"
        )

        # Export raw bucket prefix for downstream stacks
        CfnOutput(
            self, "RawPrefix",
            value=f"s3://{self.raw_bucket.bucket_name}/raw/",
            export_name=f"{self.stack_name}-RawPrefix"
        )

        # Export processed bucket prefix for downstream stacks
        CfnOutput(
            self, "ProcessedPrefix",
            value=f"s3://{self.processed_bucket.bucket_name}/processed/",
            export_name=f"{self.stack_name}-ProcessedPrefix"
        )

        # Export raw bucket prefix for downstream stacks
        CfnOutput(
            self,
            "AthenaResultsBucketName",
            value=self.athena_results_bucket.bucket_name,
            export_name="AthenaResultsBucketName",
        )

        # Export Glue database for downstream stacks
        CfnOutput(
            self, "NutrisageDBName",
            value=self.database.ref,
            export_name="NutrisageDB",
        )

        # Export processed-data crawler so other stacks can start it ──
        CfnOutput(
            self, "NutriSageProcessedCrawler",
            value=processed_crawler.ref,          # .ref returns the crawler name
            export_name="NutriSageProcessedCrawler",
        )

        # Seed raw/ prefix with a placeholder object
        # s3deploy.BucketDeployment(
        #     self,
        #     "SeedRawPrefix",
        #     destination_bucket=self.raw_bucket,
        #     destination_key_prefix="raw",
        #     sources=[
        #         s3deploy.Source.data(
        #             "_README.txt",
        #             "Place source JSONL (.jsonl.gz) files here; do not modify once uploaded."
        #         )
        #     ],
        # )

        # # Seed processed/ prefix with a placeholder object
        # s3deploy.BucketDeployment(
        #     self,
        #     "SeedProcessedPrefix",
        #     destination_bucket=self.processed_bucket,
        #     destination_key_prefix="processed",
        #     sources=[
        #         s3deploy.Source.data(
        #             "_README.txt",
        #             "Auto-generated Parquet only - written by the ingestion job."
        #         )
        #     ],
        # )

        # IAM for ingestion role
        ingest_role = iam.Role(
            self, "IngestRole",
            assumed_by=iam.ServicePrincipal("ec2.amazonaws.com"),
            description="Allows EC2 ingestion job to write to raw/ and processed/ prefixes",
        )

        # Grant write s3 permision
        ingest_role.add_to_policy(
            iam.PolicyStatement(
                actions=["s3:PutObject", "s3:AbortMultipartUpload"],
                resources=[
                    self.raw_bucket.arn_for_objects("raw/*"),
                    self.processed_bucket.arn_for_objects("processed/*"),
                    self.athena_results_bucket.arn_for_objects("athena/*"),
                ],
            )
        )

        # Cloudwatch log group for ingestion validation
        logs.LogGroup(
            self, "IngestValidationLog",
            log_group_name="/nutrisage/ingest/validation",
            retention=logs.RetentionDays.ONE_WEEK,
        )
