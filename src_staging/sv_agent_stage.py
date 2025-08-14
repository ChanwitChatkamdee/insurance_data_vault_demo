from databricks.sdk.runtime import *

from pyspark.sql import DataFrame
from pyspark.sql.types import *
from pyspark.sql.functions import *

from utils.schemas import cast_to_schema
from src_staging.bz_stage import read_bronze_table

def agent_schema():
    return StructType([
    StructField("agent_id",       StringType(),   True),
    StructField("agent_name",     StringType(),   True),
    StructField("region",         StringType(),   True),
    StructField("license_number", StringType(),   True),
    StructField("record_source",  StringType(),   True),
    StructField("event_ts",       TimestampType(),True),
    StructField("partition_date", DateType(),     True),
    StructField("_source_table",  StringType(),   True),
])
    
def sat_agent_details_schema():
    return StructType([
        StructField("license_number",     StringType(),   True),  
        StructField("agent_name",         StringType(),   True),
        StructField("region",             StringType(),   True),
        StructField("agent_id",           StringType(),   True),   
        StructField("hashdiff",           StringType(),   True),
        StructField("effective_from_ts",  TimestampType(),True),
        StructField("effective_to_ts",    TimestampType(),True),
        StructField("is_current",         BooleanType(),  True),
        StructField("record_source",      StringType(),   True),
        StructField("_ingest_ts",         TimestampType(),True),
        StructField("partition_date",     DateType(),     True)
    ])

def hub_agent_schema():
    return StructType([
        StructField("license_number", StringType(),  True), 
        StructField("record_source",  StringType(),  True),
        StructField("_ingest_ts",        TimestampType(), True),
        StructField("partition_date", DateType(),     True),
    ])
    
def create_agent_stage(
        bronze_table: str,
        partition_col: str,
        partition_date: str
    ) -> DataFrame:
    bz_df = read_bronze_table(bronze_table, partition_col, partition_date)
    agent = (bz_df
        .select(
            col("payload_json.agent.agent_id").alias("agent_id"),
            trim(col("payload_json.agent.agent_name")).alias("agent_name"),
            initcap(trim(col("payload_json.agent.region"))).alias("region"),
            col("payload_json.agent.license").alias("license_number"),
            upper(trim(col("source_system"))).alias("record_source"),
            col("event_ts"),
            col("partition_date"),
        )
        .withColumn("_source_table", lit(bronze_table))
    )

    agent = cast_to_schema(agent,agent_schema())
    return agent