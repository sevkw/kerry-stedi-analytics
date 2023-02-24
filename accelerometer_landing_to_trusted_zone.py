import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql import functions as SqlFuncs

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node Accelerometer Landing
AccelerometerLanding_node1 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://kerry-pj3-stedi-lakehouse/accelerometer/landing/"],
        "recurse": True,
    },
    transformation_ctx="AccelerometerLanding_node1",
)

# Script generated for node Customer Trusted
CustomerTrusted_node1677127231967 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://kerry-pj3-stedi-lakehouse/customer/trusted/"],
        "recurse": True,
    },
    transformation_ctx="CustomerTrusted_node1677127231967",
)

# Script generated for node Drop Duplicates
DropDuplicates_node1677128651243 = DynamicFrame.fromDF(
    CustomerTrusted_node1677127231967.toDF().dropDuplicates(["email"]),
    glueContext,
    "DropDuplicates_node1677128651243",
)

# Script generated for node Privacy Filter
PrivacyFilter_node2 = Join.apply(
    frame1=AccelerometerLanding_node1,
    frame2=DropDuplicates_node1677128651243,
    keys1=["user"],
    keys2=["email"],
    transformation_ctx="PrivacyFilter_node2",
)

# Script generated for node Drop Fields
DropFields_node1677127383456 = DropFields.apply(
    frame=PrivacyFilter_node2,
    paths=[
        "serialNumber",
        "shareWithPublicAsOfDate",
        "birthDay",
        "registrationDate",
        "shareWithResearchAsOfDate",
        "customerName",
        "email",
        "lastUpdateDate",
        "phone",
        "shareWithFriendsAsOfDate",
    ],
    transformation_ctx="DropFields_node1677127383456",
)

# Script generated for node Accelerometer Trusted
AccelerometerTrusted_node3 = glueContext.write_dynamic_frame.from_options(
    frame=DropFields_node1677127383456,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://kerry-pj3-stedi-lakehouse/accelerometer/trusted/",
        "partitionKeys": [],
    },
    transformation_ctx="AccelerometerTrusted_node3",
)

job.commit()
