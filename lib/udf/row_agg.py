from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql.column import Column, _to_java_column, _to_seq

def row_agg(col):
    sc = SparkSession.builder.getOrCreate().sparkContext
    _f = sc._jvm.com.microsoft.udaf.RowAggBoolean.apply # spark 2.3
    # _f = sc._jvm.com.microsoft.udaf.RowAggBoolean.getUDAF().apply # spark 3.0
    return Column(_f(_to_seq(sc,[col], _to_java_column)))