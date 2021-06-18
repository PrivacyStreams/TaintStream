package com.microsoft.udf

import org.apache.spark.sql.Row
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.types._

object PackUDF {
  val packString = (row_1: String, row_2: Boolean) => {
    Row(row_1, row_2)
  }

  def packStringUDF() = {
    val returnType = StructType(
      StructField("value", StringType, true) ::
      StructField("tag", BooleanType, true) ::
      Nil
    )
    udf(packString, returnType)
  }

  val packBinary = (row_1: Array[Byte], row_2: Boolean) => {
    Row(row_1, row_2)
  }

  def packBinaryUDF() = {
    val returnType = StructType(
      StructField("value", BinaryType, true) ::
        StructField("tag", BooleanType, true) ::
        Nil
    )
    udf(packBinary, returnType)
  }

  val packBoolean = (row_1: Boolean, row_2: Boolean) => {
    Row(row_1, row_2)
  }

  def packBooleanUDF() = {
    val returnType = StructType(
      StructField("value", BooleanType, true) ::
        StructField("tag", BooleanType, true) ::
        Nil
    )
    udf(packBoolean, returnType)
  }

  val packDate = (row_1: Any, row_2: Boolean) => {
    Row(row_1, row_2)
  }

  def packDateUDF() = {
    val returnType = StructType(
      StructField("value", DateType, true) ::
        StructField("tag", BooleanType, true) ::
        Nil
    )
    udf(packDate, returnType)
  }

  val packTimestamp = (row_1: Any, row_2: Boolean) => {
    Row(row_1, row_2)
  }

  def packTimestampUDF() = {
    val returnType = StructType(
      StructField("value", TimestampType, true) ::
        StructField("tag", BooleanType, true) ::
        Nil
    )
    udf(packDate, returnType)
  }

  val packDecimal = (row_1: Any, row_2: Boolean) => {
    Row(row_1, row_2)
  }

  def packDecimalUDF() = {
    val returnType = StructType(
      StructField("value", DecimalType(38, 18), true) ::
        StructField("tag", BooleanType, true) ::
        Nil
    )
    udf(packDecimal, returnType)
  }

  val packDouble = (row_1: Double, row_2: Boolean) => {
    Row(row_1, row_2)
  }

  def packDoubleUDF() = {
    val returnType = StructType(
      StructField("value", DoubleType, true) ::
        StructField("tag", BooleanType, true) ::
        Nil
    )
    udf(packDouble, returnType)
  }

  val packFloat = (row_1: Float, row_2: Boolean) => {
    Row(row_1, row_2)
  }

  def packFloatUDF() = {
    val returnType = StructType(
      StructField("value", FloatType, true) ::
        StructField("tag", BooleanType, true) ::
        Nil
    )
    udf(packFloat, returnType)
  }

  val packByte = (row_1: Byte, row_2: Boolean) => {
    Row(row_1, row_2)
  }

  def packByteUDF() = {
    val returnType = StructType(
      StructField("value", ByteType, true) ::
        StructField("tag", BooleanType, true) ::
        Nil
    )
    udf(packByte, returnType)
  }

  val packInteger = (row_1: Int, row_2: Boolean) => {
    Row(row_1, row_2)
  }

  def packIntegerUDF() = {
    val returnType = StructType(
      StructField("value", IntegerType, true) ::
        StructField("tag", BooleanType, true) ::
        Nil
    )
    udf(packInteger, returnType)
  }

  val packLong = (row_1: Long, row_2: Boolean) => {
    Row(row_1, row_2)
  }

  def packLongUDF() = {
    val returnType = StructType(
      StructField("value", LongType, true) ::
        StructField("tag", BooleanType, true) ::
        Nil
    )
    udf(packLong, returnType)
  }

  val packShort = (row_1: Short, row_2: Boolean) => {
    Row(row_1, row_2)
  }

  def packShortUDF() = {
    val returnType = StructType(
      StructField("value", ShortType, true) ::
        StructField("tag", BooleanType, true) ::
        Nil
    )
    udf(packShort, returnType)
  }

  val packNull = (row_1: Any, row_2: Boolean) => {
    Row(row_1, row_2)
  }

  def packNullUDF() = {
    val returnType = StructType(
      StructField("value", NullType, true) ::
        StructField("tag", BooleanType, true) ::
        Nil
    )
    udf(packNull, returnType)
  }

}
