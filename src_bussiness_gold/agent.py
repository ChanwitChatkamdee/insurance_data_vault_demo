from databricks.sdk.runtime import *

from pyspark.sql import DataFrame
from pyspark.sql.types import *
from pyspark.sql.functions import *

def bv_agent_book_of_business_schema():
    return StructType([
        StructField("license_number",       StringType(),   True),
        StructField("agent_name",           StringType(),   True),
        StructField("region",               StringType(),   True),
        StructField("policies_total",       IntegerType(),  True),
        StructField("policies_active",      IntegerType(),  True),
        StructField("premium_active_total", DecimalType(18,2), True),
        StructField("snapshot_ts",          TimestampType(),True)
    ])
