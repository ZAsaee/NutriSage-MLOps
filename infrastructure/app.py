from dotenv import load_dotenv
from aws_cdk import App, Environment
from data_lake_stack import DataLakeStack
import os

load_dotenv()

app = App()

env = Environment(account=os.getenv("CDK_DEFAULT_ACCOUNT"),
                  region=os.getenv("AWS_REGION"),
                  )


DataLakeStack(app, "NutriSageDataLake", env=env)
app.synth()
