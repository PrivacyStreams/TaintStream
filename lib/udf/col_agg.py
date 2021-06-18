from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql.column import Column, _to_java_column, _to_seq

def col_agg(*cols):
    arg_len = len(cols)
    sc = SparkSession.builder.getOrCreate().sparkContext
    _f = sc._jvm.com.microsoft.udf.ColAggBooleanUDF.colAggBooleanUDF(arg_len).apply
    return Column(_f(_to_seq(sc,list(cols), _to_java_column)))