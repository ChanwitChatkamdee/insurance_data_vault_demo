from databricks.sdk.runtime import *

from pyspark.sql import DataFrame
from pyspark.sql.types import *
from pyspark.sql.functions import *

from utils.schemas import cast_to_schema
from src_staging.bz_stage import read_bronze_table

def customer_schema():
    return StructType([
        StructField("customer_id",     StringType(),  True),
        StructField("full_name",       StringType(),  True),
        StructField("dob",             DateType(),    True),
        StructField("email",           StringType(),  True),
        StructField("phone",           StringType(),  True),
        StructField("city",            StringType(),  True),
        StructField("registration_ts", TimestampType(), True),
        StructField("record_source",   StringType(),  True),
        StructField("event_ts",        TimestampType(), True),
        StructField("partition_date",  DateType(),    True),
        StructField("_source_table",   StringType(),  True)
    ])
    
def sat_customer_info_schema():
    return StructType([
        StructField("customer_id", StringType(), True),         
        StructField("full_name", StringType(), True),
        StructField("dob", DateType(), True),
        StructField("email", StringType(), True),
        StructField("phone", StringType(), True),
        StructField("city", StringType(), True),
        StructField("hashdiff", StringType(), True),            
        StructField("effective_from_ts", TimestampType(), True),
        StructField("effective_to_ts", TimestampType(), True),
        StructField("is_current", BooleanType(), True),
        StructField("record_source", StringType(), True),
        StructField("_ingest_ts", TimestampType(), True),
        StructField("partition_date", DateType(), True)
    ])

def hub_customer_schema():
    return StructType([
        StructField("customer_id", StringType(), True),    
        StructField("record_source", StringType(), True),   
        StructField("_ingest_ts", TimestampType(), True),  
        StructField("partition_date", DateType(), True)     
    ])

def create_customer_stage(
        bronze_table: str,
        partition_col: str,
        partition_date: str
    ) -> DataFrame:
    bz_df = read_bronze_table(bronze_table, partition_col, partition_date)
    cust = (bz_df
        .select(
            col("payload_json.customer.customer_id").alias("customer_id"),
            trim(col("payload_json.customer.full_name")).alias("full_name"),
            to_date(col("payload_json.customer.dob")).alias("dob"),
            lower(trim(col("payload_json.customer.email"))).alias("email"),
            regexp_replace(col("payload_json.customer.phone"), r"\D+", "").alias("phone"),
            col("payload_json.customer.city").alias("city"),
            to_timestamp(col("payload_json.customer.registration_ts")).alias("registration_ts"),
            upper(trim(col("source_system"))).alias("record_source"),
            col("event_ts"),
            col("partition_date"),
        )
        .withColumn("_source_table", lit(bronze_table))
    )
    cust = cast_to_schema(cust, customer_schema())
    return cust