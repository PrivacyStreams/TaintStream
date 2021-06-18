from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql.column import Column, _to_java_column, _to_seq

def get_tag_BooleanType(col):
    sc = SparkSession.builder.getOrCreate().sparkContext
    _f = sc._jvm.com.microsoft.udf.GetTagUDF.getBooleanTagUDF().apply
    return Column(_f(_to_seq(sc,[col], _to_java_column)))