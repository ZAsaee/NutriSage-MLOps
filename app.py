from aws_cdk import App, Environment
from infrastructure.data_lake_stack import DataLakeStack
import os

app = App()
env = Environment(account=os.getenv("CDK_DEFAULT_ACCOUNT"),
                  region=os.getenv("AWS_REGION"))

DataLakeStack(app, "DataLakeStack", env=env)
app.synth()
