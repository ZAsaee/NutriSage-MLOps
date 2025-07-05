from sagemaker.workflow.pipeline import Pipeline
from sagemaker.workflow.parameters import ParameterFloat
from sagemaker.workflow.fail_step import FailStep
from sagemaker.session import Session


def get_pipeline(region: str, role: str) -> Pipeline:
    sess = Session()

    sample_fraction = ParameterFloat("SampleFraction", default_value=0.3)

    # minimal step so the pipeline is syntactically valid
    placeholder = FailStep(
        name="PlaceholderStep",
        error_message="To be replaced with real Processing/Training steps",
    )

    return Pipeline(
        name="NutriSageTrain",
        parameters=[sample_fraction],
        steps=[placeholder],          # <- no longer empty
        sagemaker_session=sess,
    )
