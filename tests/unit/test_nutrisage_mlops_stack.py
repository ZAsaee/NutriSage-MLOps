import aws_cdk as core
import aws_cdk.assertions as assertions

from nutrisage_mlops.nutrisage_mlops_stack import NutrisageMlopsStack

# example tests. To run these tests, uncomment this file along with the example
# resource in nutrisage_mlops/nutrisage_mlops_stack.py
def test_sqs_queue_created():
    app = core.App()
    stack = NutrisageMlopsStack(app, "nutrisage-mlops")
    template = assertions.Template.from_stack(stack)

#     template.has_resource_properties("AWS::SQS::Queue", {
#         "VisibilityTimeout": 300
#     })
