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

# Script generated for node Step Trainer Landing
StepTrainerLanding_node1 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://kerry-pj3-stedi-lakehouse/step_trainer/landing/"],
        "recurse": True,
    },
    transformation_ctx="StepTrainerLanding_node1",
)

# Script generated for node Customer Curated
CustomerCurated_node1677210165738 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://kerry-pj3-stedi-lakehouse/customer/curated/"],
        "recurse": True,
    },
    transformation_ctx="CustomerCurated_node1677210165738",
)

# Script generated for node Drop Duplicates
DropDuplicates_node1677210219598 = DynamicFrame.fromDF(
    CustomerCurated_node1677210165738.toDF().dropDuplicates(["serialNumber"]),
    glueContext,
    "DropDuplicates_node1677210219598",
)

# Script generated for node Rename Customer Curated Serial Number
RenameCustomerCuratedSerialNumber_node1677210250257 = RenameField.apply(
    frame=DropDuplicates_node1677210219598,
    old_name="serialNumber",
    new_name="customer_curated_serialnumber",
    transformation_ctx="RenameCustomerCuratedSerialNumber_node1677210250257",
)

# Script generated for node Join
Join_node2 = Join.apply(
    frame1=StepTrainerLanding_node1,
    frame2=RenameCustomerCuratedSerialNumber_node1677210250257,
    keys1=["serialNumber"],
    keys2=["customer_curated_serialnumber"],
    transformation_ctx="Join_node2",
)

# Script generated for node Drop Fields
DropFields_node1677210330172 = DropFields.apply(
    frame=Join_node2,
    paths=[
        "customer_curated_serialnumber",
        "birthDay",
        "registrationDate",
        "shareWithResearchAsOfDate",
        "customerName",
        "email",
        "lastUpdateDate",
        "phone",
        "shareWithFriendsAsOfDate",
        "shareWithPublicAsOfDate",
    ],
    transformation_ctx="DropFields_node1677210330172",
)

# Script generated for node Step Trainer Trusted
StepTrainerTrusted_node3 = glueContext.write_dynamic_frame.from_options(
    frame=DropFields_node1677210330172,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://kerry-pj3-stedi-lakehouse/step_trainer/trusted/",
        "partitionKeys": [],
    },
    transformation_ctx="StepTrainerTrusted_node3",
)

job.commit()
