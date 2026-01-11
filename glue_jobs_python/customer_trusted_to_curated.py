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

# Script generated for node Customer Trusted
CustomerTrusted_node1768118291538 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="customer_trusted", transformation_ctx="CustomerTrusted_node1768118291538")

# Script generated for node Accelerometer Landing
AccelerometerLanding_node1768118337316 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="accelerometer_landing", transformation_ctx="AccelerometerLanding_node1768118337316")

# Script generated for node Filter Customer Trusted only with Accelerometers
SqlQuery1021 = '''
SELECT c.*
FROM customer c
JOIN (
  SELECT DISTINCT user
  FROM accelerometer
) a
ON c.email = a.user;
'''
FilterCustomerTrustedonlywithAccelerometers_node1768118385129 = sparkSqlQuery(glueContext, query = SqlQuery1021, mapping = {"accelerometer":AccelerometerLanding_node1768118337316, "customer":CustomerTrusted_node1768118291538}, transformation_ctx = "FilterCustomerTrustedonlywithAccelerometers_node1768118385129")

# Script generated for node Customer curated
EvaluateDataQuality().process_rows(frame=FilterCustomerTrustedonlywithAccelerometers_node1768118385129, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1768118262478", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
Customercurated_node1768118714872 = glueContext.getSink(path="s3://stedi-mila-lake-house/customer/curated/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="Customercurated_node1768118714872")
Customercurated_node1768118714872.setCatalogInfo(catalogDatabase="stedi",catalogTableName="customer_curated")
Customercurated_node1768118714872.setFormat("json")
Customercurated_node1768118714872.writeFrame(FilterCustomerTrustedonlywithAccelerometers_node1768118385129)
job.commit()