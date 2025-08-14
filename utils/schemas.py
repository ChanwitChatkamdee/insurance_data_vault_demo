from databricks.sdk.runtime import *

from pyspark.sql.types import *
from pyspark.sql.functions import *

def cast_to_schema(df, schema: StructType):
    from pyspark.sql.functions import col, lit
    out = []
    names = set(df.columns)
    for f in schema.fields:
        out.append(col(f.name).cast(f.dataType) if f.name in names else lit(None).cast(f.dataType).alias(f.name))
    return df.select(*out)