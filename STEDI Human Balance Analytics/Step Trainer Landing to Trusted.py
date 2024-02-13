import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node Customer_Curated
Customer_Curated_node1688235376228 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://zanatiibucket/customer/curated/"],
        "recurse": True,
    },
    transformation_ctx="Customer_Curated_node1688235376228",
)

# Script generated for node Step_trainer_landing
Step_trainer_landing_node1 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://zanatiibucket/step_trainer/landing/"],
        "recurse": True,
    },
    transformation_ctx="Step_trainer_landing_node1",
)

# Script generated for node Renamed keys for Join
RenamedkeysforJoin_node1688235896089 = ApplyMapping.apply(
    frame=Customer_Curated_node1688235376228,
    mappings=[
        ("serialNumber", "string", "right_serialNumber", "string"),
        ("birthDay", "string", "right_birthDay", "string"),
        (
            "shareWithPublicAsOfDate",
            "bigint",
            "right_shareWithPublicAsOfDate",
            "bigint",
        ),
        (
            "shareWithResearchAsOfDate",
            "bigint",
            "right_shareWithResearchAsOfDate",
            "bigint",
        ),
        ("registrationDate", "bigint", "right_registrationDate", "bigint"),
        ("customerName", "string", "right_customerName", "string"),
        ("email", "string", "right_email", "string"),
        ("lastUpdateDate", "bigint", "right_lastUpdateDate", "bigint"),
        ("phone", "string", "right_phone", "string"),
        (
            "shareWithFriendsAsOfDate",
            "bigint",
            "right_shareWithFriendsAsOfDate",
            "bigint",
        ),
    ],
    transformation_ctx="RenamedkeysforJoin_node1688235896089",
)

# Script generated for node Join
Join_node1688234395778 = Join.apply(
    frame1=Step_trainer_landing_node1,
    frame2=RenamedkeysforJoin_node1688235896089,
    keys1=["serialNumber"],
    keys2=["right_serialNumber"],
    transformation_ctx="Join_node1688234395778",
)

# Script generated for node Drop Fields
DropFields_node1688235996403 = DropFields.apply(
    frame=Join_node1688234395778,
    paths=[
        "right_birthDay",
        "right_shareWithPublicAsOfDate",
        "right_shareWithResearchAsOfDate",
        "right_registrationDate",
        "right_customerName",
        "right_email",
        "right_lastUpdateDate",
        "right_phone",
        "right_shareWithFriendsAsOfDate",
        "right_serialNumber",
    ],
    transformation_ctx="DropFields_node1688235996403",
)

# Script generated for node Step_Trainer_Trusted
Step_Trainer_Trusted_node3 = glueContext.write_dynamic_frame.from_options(
    frame=DropFields_node1688235996403,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://zanatiibucket/step_trainer/trusted/",
        "partitionKeys": [],
    },
    transformation_ctx="Step_Trainer_Trusted_node3",
)

job.commit()
