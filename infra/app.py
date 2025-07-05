from dotenv import load_dotenv
from aws_cdk import App, Environment, Fn

from data_lake_stack import DataLakeStack
from pipeline_stack import PipelineStack
from nutrisage_train.cdk_construct import NutriSageTrainStack
import os

load_dotenv()

app = App()

env = Environment(
    account=os.getenv("CDK_DEFAULT_ACCOUNT"),
    region=os.getenv("AWS_REGION")                # e.g. "us-east-1"
)

# ── 1. Data-lake (buckets, Glue DB, outputs) ─────────────────────────
datalake = DataLakeStack(app, "NutriSageDataLake", env=env)

# ── 2. Ingestion/cleaning Pipeline (crawler, ingestion state machine, etc.) ─────────────
pipeline = PipelineStack(app, "NutriSagePipeline", env=env)
pipeline.add_dependency(datalake)  # deploy order

# ── 3. Training-pipeline (SageMaker Pipelines) stack ─────────────
exec_role_arn = Fn.import_value("NutriSageExecRoleArn")
train_stack = NutriSageTrainStack(
    app,
    "NutriSageTrainStack",
    role_arn=exec_role_arn,
    env=env
)
train_stack.add_dependency(pipeline)   # deploy order

app.synth()
