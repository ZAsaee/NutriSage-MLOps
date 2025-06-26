from pathlib import Path

from aws_cdk import (
    Stack,
    aws_s3 as s3,
    aws_glue as glue,
    aws_iam as iam,
    RemovalPolicy,
    Duration,
    CfnOutput
)
from constructs import Construct
import yaml
import os

# Helper: load column list to build the the Glue schema
print(Path(__file__).parents[1])
COLS_YAML = Path(__file__).parents[1] / "src" / "fe" / "candidate-columns.yml"
KEEP_COLS = yaml.safe_load(COLS_YAML.read_text())["columns"]
TARGET = "nutrition_grade_fr"


class DataLakeStack(Stack):
    def __init__(self, scope: Construct, cid: str, **kwargs) -> None:
        super().__init__(scope, cid, **kwargs)

        account = os.getenv("CDK_DEFAULT_ACCOUNT")
        prefix = os.getenv("PROJECT_PREFIX", "nutrisage")

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

        # Glue database
        database = glue.CfnDatabase(
            self, "DB",
            catalog_id=self.account,
            database_input=glue.CfnDatabase.DatabaseInputProperty(
                name=f"{prefix}_datalake"),
        )

        # Glue table using the same processed bucket
        table = glue.CfnTable(
            self, "Table",
            catalog_id=self.account,
            database_name=database.ref,
            table_input=glue.CfnTable.TableInputProperty(
                name="foodfacts_raw",
                table_type="EXTERNAL_TABLE",
                partition_keys=[
                    glue.CfnTable.ColumnProperty(name="year", type="int"),
                    glue.CfnTable.ColumnProperty(
                        name="country", type="string"),
                ],
                storage_descriptor=glue.CfnTable.StorageDescriptorProperty(
                    location=f"s3://{self.processed_bucket.bucket_name}/processed/",
                    input_format="org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat",
                    output_format="org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat",
                    serde_info=glue.CfnTable.SerdeInfoProperty(
                        serialization_library="org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe",
                    ),
                    columns=[
                        glue.CfnTable.ColumnProperty(name=c, type="string") for c in KEEP_COLS
                    ],
                ),
            ),
        )

        CfnOutput(
            self, "RawBucketName",
            value=self.raw_bucket.bucket_name,
            export_name=f"{self.stack_name}-raw-bucket"
        )

        CfnOutput(
            self, "ProcessedBucketName",
            value=self.processed_bucket.bucket_name,
            export_name=f"{self.stack_name}-processed-bucket"
        )
