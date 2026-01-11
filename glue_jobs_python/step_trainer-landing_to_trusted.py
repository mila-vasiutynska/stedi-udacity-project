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

# Script generated for node Step Trainer Landing
StepTrainerLanding_node1768120770957 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "false"}, connection_type="s3", format="json", connection_options={"paths": ["s3://stedi-mila-lake-house/step_trainer/landing/"], "recurse": True}, transformation_ctx="StepTrainerLanding_node1768120770957")

# Script generated for node Customer Curated
CustomerCurated_node1768120099011 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="customer_curated", transformation_ctx="CustomerCurated_node1768120099011")

# Script generated for node Filter only Customer Curated
SqlQuery848 = '''
select st.* from stepTrainer st
join customer c on
st.serialnumber=c.serialnumber;
'''
FilteronlyCustomerCurated_node1768120014820 = sparkSqlQuery(glueContext, query = SqlQuery848, mapping = {"customer":CustomerCurated_node1768120099011, "stepTrainer":StepTrainerLanding_node1768120770957}, transformation_ctx = "FilteronlyCustomerCurated_node1768120014820")

# Script generated for node Step Trainer Trusted
EvaluateDataQuality().process_rows(frame=FilteronlyCustomerCurated_node1768120014820, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1768119723197", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
StepTrainerTrusted_node1768120351472 = glueContext.getSink(path="s3://stedi-mila-lake-house/step_trainer/trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="StepTrainerTrusted_node1768120351472")
StepTrainerTrusted_node1768120351472.setCatalogInfo(catalogDatabase="stedi",catalogTableName="step_trainer_trusted")
StepTrainerTrusted_node1768120351472.setFormat("json")
StepTrainerTrusted_node1768120351472.writeFrame(FilteronlyCustomerCurated_node1768120014820)
job.commit()