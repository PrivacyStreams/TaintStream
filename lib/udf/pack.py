from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql.column import Column, _to_java_column, _to_seq


def pack_IntegerType(col_1, col_2):
    sc = SparkSession.builder.getOrCreate().sparkContext
    _f = sc._jvm.com.microsoft.udf.PackUDF.packIntegerUDF().apply
    return Column(_f(_to_seq(sc,[col_1, col_2], _to_java_column)))


def pack_StringType(col_1, col_2):
    sc = SparkSession.builder.getOrCreate().sparkContext
    _f = sc._jvm.com.microsoft.udf.PackUDF.packStringUDF().apply
    return Column(_f(_to_seq(sc,[col_1, col_2], _to_java_column)))


def pack_BinaryType(col_1, col_2):
    sc = SparkSession.builder.getOrCreate().sparkContext
    _f = sc._jvm.com.microsoft.udf.PackUDF.packBinaryUDF().apply
    return Column(_f(_to_seq(sc,[col_1, col_2], _to_java_column)))


def pack_BooleanType(col_1, col_2):
    sc = SparkSession.builder.getOrCreate().sparkContext
    _f = sc._jvm.com.microsoft.udf.PackUDF.packBooleanUDF().apply
    return Column(_f(_to_seq(sc,[col_1, col_2], _to_java_column)))


def pack_DateType(col_1, col_2):
    sc = SparkSession.builder.getOrCreate().sparkContext
    _f = sc._jvm.com.microsoft.udf.PackUDF.packDateUDF().apply
    return Column(_f(_to_seq(sc,[col_1, col_2], _to_java_column)))


def pack_TimestampType(col_1, col_2):
    sc = SparkSession.builder.getOrCreate().sparkContext
    _f = sc._jvm.com.microsoft.udf.PackUDF.packTimestampUDF().apply
    return Column(_f(_to_seq(sc,[col_1, col_2], _to_java_column)))


def pack_DecimalType(col_1, col_2):
    sc = SparkSession.builder.getOrCreate().sparkContext
    _f = sc._jvm.com.microsoft.udf.PackUDF.packDecimalUDF().apply
    return Column(_f(_to_seq(sc,[col_1, col_2], _to_java_column)))


def pack_DoubleType(col_1, col_2):
    sc = SparkSession.builder.getOrCreate().sparkContext
    _f = sc._jvm.com.microsoft.udf.PackUDF.packDoubleUDF().apply
    return Column(_f(_to_seq(sc,[col_1, col_2], _to_java_column)))


def pack_FloatType(col_1, col_2):
    sc = SparkSession.builder.getOrCreate().sparkContext
    _f = sc._jvm.com.microsoft.udf.PackUDF.packFloatUDF().apply
    return Column(_f(_to_seq(sc,[col_1, col_2], _to_java_column)))


def pack_ByteType(col_1, col_2):
    sc = SparkSession.builder.getOrCreate().sparkContext
    _f = sc._jvm.com.microsoft.udf.PackUDF.packByteUDF().apply
    return Column(_f(_to_seq(sc,[col_1, col_2], _to_java_column)))


def pack_LongType(col_1, col_2):
    sc = SparkSession.builder.getOrCreate().sparkContext
    _f = sc._jvm.com.microsoft.udf.PackUDF.packLongUDF().apply
    return Column(_f(_to_seq(sc,[col_1, col_2], _to_java_column)))


def pack_ShortType(col_1, col_2):
    sc = SparkSession.builder.getOrCreate().sparkContext
    _f = sc._jvm.com.microsoft.udf.PackUDF.packShortUDF().apply
    return Column(_f(_to_seq(sc,[col_1, col_2], _to_java_column)))


def pack_NullType(col_1, col_2):
    sc = SparkSession.builder.getOrCreate().sparkContext
    _f = sc._jvm.com.microsoft.udf.PackUDF.packNullUDF().apply
    return Column(_f(_to_seq(sc,[col_1, col_2], _to_java_column)))