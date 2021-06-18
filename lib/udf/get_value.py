from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql.column import Column, _to_java_column, _to_seq

def get_value_IntegerType(col):
    sc = SparkSession.builder.getOrCreate().sparkContext
    _f = sc._jvm.com.microsoft.udf.GetValueUDF.getIntegerValueUDF().apply
    return Column(_f(_to_seq(sc,[col], _to_java_column)))


def get_value_StringType(col):
    sc = SparkSession.builder.getOrCreate().sparkContext
    _f = sc._jvm.com.microsoft.udf.GetValueUDF.getStringValueUDF().apply
    return Column(_f(_to_seq(sc,[col], _to_java_column)))


def get_value_BinaryType(col):
    sc = SparkSession.builder.getOrCreate().sparkContext
    _f = sc._jvm.com.microsoft.udf.GetValueUDF.getBinaryValueUDF().apply
    return Column(_f(_to_seq(sc,[col], _to_java_column)))


def get_value_BooleanType(col):
    sc = SparkSession.builder.getOrCreate().sparkContext
    _f = sc._jvm.com.microsoft.udf.GetValueUDF.getBooleanValueUDF().apply
    return Column(_f(_to_seq(sc,[col], _to_java_column)))


def get_value_DateType(col):
    sc = SparkSession.builder.getOrCreate().sparkContext
    _f = sc._jvm.com.microsoft.udf.GetValueUDF.getDateValueUDF().apply
    return Column(_f(_to_seq(sc,[col], _to_java_column)))


def get_value_TimestampType(col):
    sc = SparkSession.builder.getOrCreate().sparkContext
    _f = sc._jvm.com.microsoft.udf.GetValueUDF.getTimestampValueUDF().apply
    return Column(_f(_to_seq(sc,[col], _to_java_column)))


def get_value_DecimalType(col):
    sc = SparkSession.builder.getOrCreate().sparkContext
    _f = sc._jvm.com.microsoft.udf.GetValueUDF.getDecimalValueUDF().apply
    return Column(_f(_to_seq(sc,[col], _to_java_column)))


def get_value_DoubleType(col):
    sc = SparkSession.builder.getOrCreate().sparkContext
    _f = sc._jvm.com.microsoft.udf.GetValueUDF.getDoubleValueUDF().apply
    return Column(_f(_to_seq(sc,[col], _to_java_column)))


def get_value_FloatType(col):
    sc = SparkSession.builder.getOrCreate().sparkContext
    _f = sc._jvm.com.microsoft.udf.GetValueUDF.getFloatValueUDF().apply
    return Column(_f(_to_seq(sc,[col], _to_java_column)))


def get_value_ByteType(col):
    sc = SparkSession.builder.getOrCreate().sparkContext
    _f = sc._jvm.com.microsoft.udf.GetValueUDF.getByteValueUDF().apply
    return Column(_f(_to_seq(sc,[col], _to_java_column)))


def get_value_ShortType(col):
    sc = SparkSession.builder.getOrCreate().sparkContext
    _f = sc._jvm.com.microsoft.udf.GetValueUDF.getShortValueUDF().apply
    return Column(_f(_to_seq(sc,[col], _to_java_column)))


def get_value_LongType(col):
    sc = SparkSession.builder.getOrCreate().sparkContext
    _f = sc._jvm.com.microsoft.udf.GetValueUDF.getLongValueUDF().apply
    return Column(_f(_to_seq(sc,[col], _to_java_column)))

def get_value_NullType(col):
    sc = SparkSession.builder.getOrCreate().sparkContext
    _f = sc._jvm.com.microsoft.udf.GetValueUDF.getNullValueUDF().apply
    return Column(_f(_to_seq(sc,[col], _to_java_column)))
