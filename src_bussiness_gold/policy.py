from pyspark.sql.types import *

def bv_policy_current_status_schema():
    return StructType([
        StructField("policy_number",     StringType(),   True),
        StructField("current_status",    StringType(),   True), 
        StructField("current_status_ts", TimestampType(),True),
        StructField("start_date",        DateType(),     True),
        StructField("end_date",          DateType(),     True),
        StructField("updated_ts",        TimestampType(),True)    
    ])

def bv_pit_policy_daily_schema():
    return StructType([
        StructField("policy_number",     StringType(),   True),
        StructField("partition_date",    DateType(),     True), 
        StructField("policy_type",       StringType(),   True),
        StructField("coverage_amount",   DecimalType(18,2), True),
        StructField("premium_amount",    DecimalType(18,2), True),
        StructField("start_date",        DateType(),     True),
        StructField("end_date",          DateType(),     True),
        StructField("current_status",    StringType(),   True),
        StructField("current_status_ts", TimestampType(),True),
        StructField("is_active",         BooleanType(),  True),
        StructField("snapshot_ts",       TimestampType(),True)
    ])