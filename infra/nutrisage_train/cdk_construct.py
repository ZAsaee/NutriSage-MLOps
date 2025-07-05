
from aws_cdk import Stack, aws_sagemaker as sm
from constructs import Construct
import nutrisage_train.pipeline as pl            # ← your SDK builder


class NutriSageTrainStack(Stack):
    def __init__(self, scope: Construct, cid: str, *, role_arn: str, **kw) -> None:
        super().__init__(scope, cid, **kw)

        # 1 Build the SDK pipeline (includes placeholder FailStep)
        pipeline = pl.get_pipeline(region=self.region, role=role_arn)

        # 2 **DO NOT json.dumps()**  – definition() is already a JSON string
        definition_str = pipeline.definition()    # <- ready for CFN

        # 3 Feed that string directly to CloudFormation
        sm.CfnPipeline(
            self,
            "NutriSageTrain",
            pipeline_name=pipeline.name,
            role_arn=role_arn,
            pipeline_definition={
                "PipelineDefinitionBody": definition_str   # exactly one encoding
            },
        )
