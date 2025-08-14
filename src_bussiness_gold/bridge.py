from databricks.sdk.runtime import *

from pyspark.sql import DataFrame
from pyspark.sql.types import *
from pyspark.sql.functions import *

def bv_bridge_agent_current_schema():
    return StructType([
        StructField("customer_id",    StringType(), True),
        StructField("license_number", StringType(), True),
        StructField("snapshot_ts",    TimestampType(), True)
    ])