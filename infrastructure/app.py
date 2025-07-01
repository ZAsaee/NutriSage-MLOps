from dotenv import load_dotenv
from aws_cdk import App, Environment
from data_lake_stack import DataLakeStack
from pipeline_stack import PipelineStack
import os

load_dotenv()

app = App()

env = Environment(
    account=os.getenv("CDK_DEFAULT_ACCOUNT"),
    region=os.getenv("AWS_REGION")                # e.g. "us-east-1"
)

# ── 1. Data-lake (buckets, Glue DB, outputs) ─────────────────────────
datalake = DataLakeStack(app, "NutriSageDataLake", env=env)

# ── 2. Pipeline (crawler, ingestion state machine, etc.) ─────────────
pipeline = PipelineStack(app, "NutriSagePipeline", env=env)

# Ensure pipeline deploys after data-lake (exports are available)
pipeline.add_dependency(datalake)

app.synth()
