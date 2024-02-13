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

# Script generated for node Step Trainter Trusted
StepTrainterTrusted_node1 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://zanatiibucket/step_trainer/trusted/"],
        "recurse": True,
    },
    transformation_ctx="StepTrainterTrusted_node1",
)

# Script generated for node Accelerometer Trusted
AccelerometerTrusted_node1688237707757 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://zanatiibucket/accelerometer/trusted/"],
        "recurse": True,
    },
    transformation_ctx="AccelerometerTrusted_node1688237707757",
)

# Script generated for node Customer Curated
CustomerCurated_node1688237709736 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://zanatiibucket/customer/curated/"],
        "recurse": True,
    },
    transformation_ctx="CustomerCurated_node1688237709736",
)

# Script generated for node Renamed keys for Join
RenamedkeysforJoin_node1688238233116 = ApplyMapping.apply(
    frame=StepTrainterTrusted_node1,
    mappings=[
        ("serialNumber", "string", "right_serialNumber", "string"),
        ("sensorReadingTime", "long", "right_sensorReadingTime", "long"),
        ("distanceFromObject", "int", "right_distanceFromObject", "int"),
    ],
    transformation_ctx="RenamedkeysforJoin_node1688238233116",
)

# Script generated for node Join
Join_node1688237789880 = Join.apply(
    frame1=CustomerCurated_node1688237709736,
    frame2=AccelerometerTrusted_node1688237707757,
    keys1=["email"],
    keys2=["user"],
    transformation_ctx="Join_node1688237789880",
)

# Script generated for node Drop Fields
DropFields_node1688238557368 = DropFields.apply(
    frame=Join_node1688237789880,
    paths=[
        "birthDay",
        "shareWithPublicAsOfDate",
        "shareWithResearchAsOfDate",
        "registrationDate",
        "customerName",
        "email",
        "lastUpdateDate",
        "phone",
        "shareWithFriendsAsOfDate",
    ],
    transformation_ctx="DropFields_node1688238557368",
)

# Script generated for node Join
Join_node1688238023832 = Join.apply(
    frame1=RenamedkeysforJoin_node1688238233116,
    frame2=DropFields_node1688238557368,
    keys1=["right_serialNumber", "right_sensorReadingTime"],
    keys2=["serialNumber", "timeStamp"],
    transformation_ctx="Join_node1688238023832",
)

# Script generated for node Drop Fields
DropFields_node1688238669115 = DropFields.apply(
    frame=Join_node1688238023832,
    paths=["serialNumber"],
    transformation_ctx="DropFields_node1688238669115",
)

# Script generated for node Machine Learning Curated
MachineLearningCurated_node3 = glueContext.write_dynamic_frame.from_options(
    frame=DropFields_node1688238669115,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://zanatiibucket/machineLearning/curated/",
        "partitionKeys": [],
    },
    transformation_ctx="MachineLearningCurated_node3",
)

job.commit()
