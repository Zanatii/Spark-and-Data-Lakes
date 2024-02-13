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

# Script generated for node Accelerometer Landing
AccelerometerLanding_node1688164632255 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="accelerometer_landing",
    transformation_ctx="AccelerometerLanding_node1688164632255",
)

# Script generated for node Customer Trusted Zone
CustomerTrustedZone_node1 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="customer_trusted",
    transformation_ctx="CustomerTrustedZone_node1",
)

# Script generated for node Join Customer
JoinCustomer_node1688164646827 = Join.apply(
    frame1=CustomerTrustedZone_node1,
    frame2=AccelerometerLanding_node1688164632255,
    keys1=["email"],
    keys2=["user"],
    transformation_ctx="JoinCustomer_node1688164646827",
)

# Script generated for node Drop Fields
DropFields_node1688164654085 = DropFields.apply(
    frame=JoinCustomer_node1688164646827,
    paths=["user", "timestamp", "x", "y", "z"],
    transformation_ctx="DropFields_node1688164654085",
)

# Script generated for node Customer Curated
CustomerCurated_node3 = glueContext.write_dynamic_frame.from_options(
    frame=DropFields_node1688164654085,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://zanatiibucket/customer/curated/",
        "partitionKeys": [],
    },
    transformation_ctx="CustomerCurated_node3",
)

job.commit()
