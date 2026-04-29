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

# Script generated for node Amazon S3
AmazonS3_node1777439766212 = glueContext.create_dynamic_frame.from_options(format_options={"quoteChar": "\"", "withHeader": True, "separator": ",", "multiLine": "false", "optimizePerformance": False}, connection_type="s3", format="csv", connection_options={"paths": ["s3://boxy/Raw/customer_orders/"], "recurse": True}, transformation_ctx="AmazonS3_node1777439766212")

# Script generated for node Selecting the columns
Selectingthecolumns_node1777439931526 = SelectFields.apply(frame=AmazonS3_node1777439766212, paths=["customer_name", "city", "product", "category", "price", "quantity", "order_status", "order_date", "order_id", "customer_id"], transformation_ctx="Selectingthecolumns_node1777439931526")

# Script generated for node Change Schema
ChangeSchema_node1777440673503 = ApplyMapping.apply(frame=Selectingthecolumns_node1777439931526, mappings=[("order_id", "string", "order_id", "string"), ("customer_id", "string", "customer_id", "string"), ("customer_name", "string", "customer_name", "string"), ("city", "string", "city", "string"), ("product", "string", "product", "string"), ("category", "string", "category", "string"), ("price", "string", "price", "string"), ("quantity", "string", "quantity", "string"), ("order_status", "string", "order_status", "string"), ("order_date", "string", "order_date", "string")], transformation_ctx="ChangeSchema_node1777440673503")

# Script generated for node SQL Query
SqlQuery277 = '''
SELECT *,
       price * quantity AS total_amount
FROM myDataSource
'''
SQLQuery_node1777440023011 = sparkSqlQuery(glueContext, query = SqlQuery277, mapping = {"myDataSource":ChangeSchema_node1777440673503}, transformation_ctx = "SQLQuery_node1777440023011")

# Script generated for node Selecting the transfomed columns
Selectingthetransfomedcolumns_node1777440208315 = SelectFields.apply(frame=SQLQuery_node1777440023011, paths=["order_id", "customer_id", "customer_name", "city", "product", "category", "price", "quantity", "order_status", "order_date", "total_amount"], transformation_ctx="Selectingthetransfomedcolumns_node1777440208315")

# Script generated for node Amazon S3
EvaluateDataQuality().process_rows(frame=Selectingthetransfomedcolumns_node1777440208315, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1777439741513", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
AmazonS3_node1777440170839 = glueContext.write_dynamic_frame.from_options(frame=Selectingthetransfomedcolumns_node1777440208315, connection_type="s3", format="glueparquet", connection_options={"path": "s3://datalakedestination/silver/customer_orders/", "partitionKeys": ["order_date"]}, format_options={"compression": "snappy"}, transformation_ctx="AmazonS3_node1777440170839")

job.commit()