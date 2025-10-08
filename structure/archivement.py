from diagrams import Diagram, Cluster
from diagrams.aws.compute import Lambda
from diagrams.aws.integration import StepFunctions, SNS, Eventbridge
from diagrams.aws.analytics import Glue, Athena
from diagrams.aws.storage import S3
from diagrams.aws.management import Cloudwatch
from diagrams.aws.general import Client, User

with Diagram("AWS", show=False, direction="TB", filename="structure/structure_img/diagrams") as diag:
    eventbridge = Eventbridge("EventBridge scheduler")
    user = User("Data Analyst")

    with Cluster("Work Flow"):
        sfn = StepFunctions("Step Functions (orchestration)")

        eventbridge >> sfn
    
        with Cluster("Pipe Line"):
            lambda_handler = Lambda('Scraper\n(YouTube API)')
            glue_job = Glue("Glue Job\n(Transform Data)")
            s3 = S3("S3\n(Data Lake)")

            sfn >> lambda_handler >> s3 >> glue_job

            glue_job >> s3

        athena = Athena("Athena\n(Data Catalogue)")

        glue_job >> athena >> user

    with Cluster("Monitoring Quality Check"):
        sns = SNS("SNS Topic")
        
        alarms = [
            Cloudwatch("SFN ExecutionsFailed"),
            Cloudwatch("SFN Duration (>15 minutes)"),
            Cloudwatch("DQ Failure")
        ]
        
        for alarm in alarms:
            alarm >> sns

        sns >> user 

    sfn >> alarms[0]
    sfn >> alarms[1]
    glue_job >> alarms[2]

