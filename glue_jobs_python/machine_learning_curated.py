import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsgluedq.transforms import EvaluateDataQuality
from awsglue import DynamicFrame

def sparkSqlQuery(glueContext, query, mapping, transformation_ctx) -> DynamicFrame:
    for alias, frame in mapping.items():
        frame.toDF().createOrReplaceTempView(alias)
    result = spark.sql(query)
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Default ruleset used by all target nodes with data quality enabled
DEFAULT_DATA_QUALITY_RULESET = """
    Rules = [
        ColumnCount > 0
    ]
"""

# Script generated for node Step Trainer Trusted
StepTrainerTrusted_node1768121201335 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="step_trainer_trusted", transformation_ctx="StepTrainerTrusted_node1768121201335")

# Script generated for node Customer Curated
CustomerCurated_node1768121279192 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="customer_curated", transformation_ctx="CustomerCurated_node1768121279192")

# Script generated for node Accelerometer Trusted
AccelerometerTrusted_node1768121306569 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="accelerometer_trusted", transformation_ctx="AccelerometerTrusted_node1768121306569")

# Script generated for node SQL Query
SqlQuery707 = '''
SELECT
  ac.customername,
  ac.email,
  ac.phone,
  ac.birthday,
  ac.registrationdate,
  ac.lastupdatedate,
  ac.sharewithpublicasofdate,
  ac.sharewithresearchasofdate,
  ac.sharewithfriendsasofdate,
  st.*,
  ac.x,
  ac.y,
  ac.z
FROM stepTrainer st
JOIN (
  SELECT
    c.customername,
    c.email,
    c.phone,
    c.birthday,
    c.registrationdate,
    c.lastupdatedate,
    c.sharewithpublicasofdate,
    c.sharewithresearchasofdate,
    c.sharewithfriendsasofdate,
    c.serialnumber,
    a.timestamp,
    a.x,
    a.y,
    a.z
  FROM accelerometer a
  JOIN customer c
    ON c.email = a.user
) ac
  ON st.serialnumber = ac.serialnumber
 AND st.sensorReadingTime = ac.timestamp;
'''
SQLQuery_node1768121343527 = sparkSqlQuery(glueContext, query = SqlQuery707, mapping = {"stepTrainer":StepTrainerTrusted_node1768121201335, "customer":CustomerCurated_node1768121279192, "accelerometer":AccelerometerTrusted_node1768121306569}, transformation_ctx = "SQLQuery_node1768121343527")

# Script generated for node machine_learning_curated
EvaluateDataQuality().process_rows(frame=SQLQuery_node1768121343527, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1768121195800", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
machine_learning_curated_node1768122050467 = glueContext.getSink(path="s3://stedi-mila-lake-house/machine_learning/curated/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="machine_learning_curated_node1768122050467")
machine_learning_curated_node1768122050467.setCatalogInfo(catalogDatabase="stedi",catalogTableName="machine_learning_curated")
machine_learning_curated_node1768122050467.setFormat("json")
machine_learning_curated_node1768122050467.writeFrame(SQLQuery_node1768121343527)
job.commit()