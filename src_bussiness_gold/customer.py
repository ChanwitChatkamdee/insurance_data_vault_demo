from databricks.sdk.runtime import *

from pyspark.sql import DataFrame
from pyspark.sql.types import *
from pyspark.sql.functions import *

def bv_customer_360_schema() -> StructType:
    return StructType([
        StructField("customer_id", StringType(), True),
        StructField("full_name",   StringType(), True),
        StructField("dob",         DateType(),   True),
        StructField("email",       StringType(), True),
        StructField("phone",       StringType(), True),
        StructField("city",        StringType(), True),
        StructField("first_seen_ts", TimestampType(), True),
        StructField("policies_total", IntegerType(), True),
        StructField("policies_active", IntegerType(), True),
        StructField("last_status_ts", TimestampType(), True),
    ])

def bv_bridge_agent_current_schema():
    return StructType([
        StructField("customer_id",    StringType(), True),
        StructField("license_number", StringType(), True),
        StructField("snapshot_ts",    TimestampType(), True)
    ])