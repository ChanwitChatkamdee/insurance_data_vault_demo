from databricks.sdk.runtime import *

from pyspark.sql import DataFrame
from pyspark.sql.types import *
from pyspark.sql.functions import *

from utils.schemas import cast_to_schema
from src_staging.bz_stage import read_bronze_table

def hub_policy_schema() -> StructType:
    return StructType([
        StructField("policy_number", StringType(),    True),  
        StructField("record_source", StringType(),    True),
        StructField("_ingest_ts",       TimestampType(), True),
        StructField("partition_date", DateType(),     True),
    ])

def sat_policy_details_schema() -> StructType:
    return StructType([
        StructField("policy_number",     StringType(),     True), 
        StructField("policy_type",       StringType(),     True),
        StructField("coverage_amount",   DecimalType(18,2),True),
        StructField("premium_amount",    DecimalType(18,2),True),
        StructField("start_date",        DateType(),       True),
        StructField("end_date",          DateType(),       True),

        # SCD2 mechanics
        StructField("hashdiff",          StringType(),     True),
        StructField("effective_from_ts", TimestampType(),  True),
        StructField("effective_to_ts",   TimestampType(),  True),
        StructField("is_current",        BooleanType(),    True),

        # provenance
        StructField("record_source",     StringType(),     True),
        StructField("_ingest_ts",        TimestampType(),  True),
        StructField("partition_date",    DateType(),       True),
    ])

def sat_policy_status_event_schema():
    return StructType([
        StructField("policy_number", StringType(),    True),
        StructField("status",        StringType(),    True),   
        StructField("status_ts",     TimestampType(), True),   
        StructField("record_source", StringType(),    True),
        StructField("_ingest_ts",    TimestampType(), True),
        StructField("partition_date",DateType(),      True),
    ])

def link_customer_policy_schema() -> StructType:
    return StructType([
        StructField("customer_id",   StringType(),    True),  
        StructField("policy_number", StringType(),    True),  
        StructField("record_source", StringType(),    True),
        StructField("_ingest_ts",       TimestampType(), True),  
        StructField("partition_date",DateType(),      True),  
    ])

def link_policy_agent_schema() -> StructType:
    return StructType([
        StructField("policy_number",  StringType(),    True), 
        StructField("license_number", StringType(),    True),  
        StructField("record_source",  StringType(),    True),
        StructField("_ingest_ts",        TimestampType(), True),
        StructField("partition_date", DateType(),      True),
    ])

def create_policy_stage(
        bronze_table: str,
        partition_col: str,
        partition_date: str
    ) -> DataFrame:
    bz_df = read_bronze_table(bronze_table, partition_col, partition_date)
    pol = (bz_df
    .select(
            col("payload_json.customer.customer_id").alias("customer_id"),
            col("payload_json.agent.license").alias("license_number"),        
            col("payload_json.agent.agent_id").alias("agent_id"),         
            explode_outer(col("payload_json.policies")).alias("p"),
            upper(trim(col("source_system"))).alias("record_source"),
            col("event_ts"),
            col("partition_date"),
        )
    .select(
            "customer_id",
            "license_number",
            "agent_id",
            trim(col("p.policy_number")).alias("policy_number"),
            upper(trim(col("p.policy_type"))).alias("policy_type"),
            col("p.coverage_amount").cast("decimal(18,2)").alias("coverage_amount"),
            col("p.premium_amount").cast("decimal(18,2)").alias("premium_amount"),
            to_date(col("p.start_date")).alias("start_date"),
            to_date(col("p.end_date")).alias("end_date"),
            col("p.status_history").alias("status_history"),
            "record_source","event_ts","partition_date"
        )
        .withColumn("_source_table", lit(bronze_table))
    )
    return pol