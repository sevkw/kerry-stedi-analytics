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

# Script generated for node Customer Trusted
CustomerTrusted_node1677208226471 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://kerry-pj3-stedi-lakehouse/customer/trusted/"],
        "recurse": True,
    },
    transformation_ctx="CustomerTrusted_node1677208226471",
)

# Script generated for node Distinct Serial Number
DistinctSerialNumber_node2 = DynamicFrame.fromDF(
    StepTrainerLanding_node1.toDF().dropDuplicates(["serialNumber"]),
    glueContext,
    "DistinctSerialNumber_node2",
)

# Script generated for node Renamed keys for Join
RenamedkeysforJoin_node1677208404269 = ApplyMapping.apply(
    frame=DistinctSerialNumber_node2,
    mappings=[
        ("sensorReadingTime", "long", "sensorReadingTime", "long"),
        ("serialNumber", "string", "step_trainer_serialNumber", "string"),
        ("distanceFromObject", "int", "distanceFromObject", "int"),
    ],
    transformation_ctx="RenamedkeysforJoin_node1677208404269",
)

# Script generated for node Join
Join_node1677208268220 = Join.apply(
    frame1=RenamedkeysforJoin_node1677208404269,
    frame2=CustomerTrusted_node1677208226471,
    keys1=["step_trainer_serialNumber"],
    keys2=["serialNumber"],
    transformation_ctx="Join_node1677208268220",
)

# Script generated for node Drop Fields
DropFields_node1677208509740 = DropFields.apply(
    frame=Join_node1677208268220,
    paths=["distanceFromObject", "sensorReadingTime", "step_trainer_serialNumber"],
    transformation_ctx="DropFields_node1677208509740",
)

# Script generated for node Customer Curated
CustomerCurated_node3 = glueContext.write_dynamic_frame.from_options(
    frame=DropFields_node1677208509740,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://kerry-pj3-stedi-lakehouse/customer/curated/",
        "partitionKeys": [],
    },
    transformation_ctx="CustomerCurated_node3",
)

job.commit()
