from databricks.sdk.runtime import *

from pyspark.sql import DataFrame
from pyspark.sql.types import *
from pyspark.sql.functions import *

def payload_schema():
    return StructType([
        StructField("customer", StructType([
            StructField("customer_id", StringType()),
            StructField("full_name",   StringType()),
            StructField("dob",         StringType()),
            StructField("email",       StringType()),
            StructField("phone",       StringType()),
            StructField("city",        StringType()),
            StructField("registration_ts", StringType())
        ])),
        StructField("policies", ArrayType(StructType([
            StructField("policy_number",   StringType()),
            StructField("policy_type",     StringType()),
            StructField("coverage_amount", StringType()),
            StructField("premium_amount",  StringType()),
            StructField("start_date",      StringType()),
            StructField("end_date",        StringType()),
            StructField("status_history",  ArrayType(StructType([
                StructField("status", StringType()),
                StructField("ts",     StringType())
            ])))
        ]))),
        StructField("agent", StructType([
            StructField("agent_id",   StringType()),
            StructField("agent_name", StringType()),
            StructField("region",     StringType()),
            StructField("license",    StringType()),
        ])),
        StructField("marketing_opt_in", StringType()),
        StructField("source_file",      StringType()),
    ])

def read_bronze_table(        
        bronze_table: str,
        partition_col: str,
        partition_date: str
    )-> DataFrame:
    df = spark.sql(f"""
            SELECT *
            FROM {bronze_table}
            WHERE {partition_col} = DATE('{partition_date}')
        """)

    parsed = (df
            .withColumn("payload_json", from_json(col("payload"), payload_schema()))
            .withColumn("event_ts", to_timestamp("event_ts"))
        )
    return parsed