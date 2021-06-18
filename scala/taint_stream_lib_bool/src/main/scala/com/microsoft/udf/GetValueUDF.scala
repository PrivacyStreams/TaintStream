package com.microsoft.udf

import org.apache.spark.sql.Row
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.types._

object GetValueUDF {
  val getStringValue = (row: Row) => {
    if (row == null) {
      null
    }
    else{
      val valueIndex = row.schema.fieldIndex("value")
      row.getString(valueIndex)
    }
  }

  def getStringValueUDF() = {
    udf(getStringValue, StringType)
  }

  val getBinaryValue = (row: Row) => {
    if (row == null) {
      null
    }
    else {
      val valueIndex = row.schema.fieldIndex("value")
      row.getAs[Array[Byte]](valueIndex)
    }
  }

  def getBinaryValueUDF() = {
    udf(getBinaryValue, BinaryType)
  }

  val getBooleanValue = (row: Row) => {
    if (row == null) {
      null
    } else {
      val valueIndex = row.schema.fieldIndex("value")
      row.getBoolean(valueIndex)
    }

  }

  def getBooleanValueUDF() = {
    udf(getBooleanValue, BooleanType)
  }

  val getDateValue = (row: Row) => {
    if (row == null) {
      null
    } else {
      val valueIndex = row.schema.fieldIndex("value")
      row.getDate(valueIndex)
    }

  }

  def getDateValueUDF() = {
    udf(getDateValue, DateType)
  }

  val getTimestampValue = (row: Row) => {
    if (row == null) {
      null
    } else {
      val valueIndex = row.schema.fieldIndex("value")
      row.getTimestamp(valueIndex)
    }
  }

  def getTimestampValueUDF() = {
    udf(getTimestampValue, TimestampType)
  }

  val getDecimalValue = (row: Row) => {
    if (row == null) {
      null
    } else {
      val valueIndex = row.schema.fieldIndex("value")
      row.getDecimal(valueIndex)
    }
  }

  def getDecimalValueUDF() = {
    udf(getDecimalValue, DecimalType(38,18))
  }

  val getDoubleValue = (row: Row) => {
    if (row == null) {
      null
    } else {
      val valueIndex = row.schema.fieldIndex("value")
      row.getDouble(valueIndex)
    }

  }

  def getDoubleValueUDF() = {
    udf(getDoubleValue, DoubleType)
  }

  val getFloatValue = (row: Row) => {
    if (row == null) {
      null
    } else {
      val valueIndex = row.schema.fieldIndex("value")
      row.getFloat(valueIndex)
    }

  }

  def getFloatValueUDF() = {
    udf(getFloatValue, FloatType)
  }

  val getByteValue = (row: Row) => {
    if (row == null) {
      null
    } else {
      val valueIndex = row.schema.fieldIndex("value")
      row.getByte(valueIndex)
    }

  }

  def getByteValueUDF() = {
    udf(getByteValue, ByteType)
  }

  val getIntegerValue = (row: Row) => {
    if (row == null) {
      null
    } else {
      val valueIndex = row.schema.fieldIndex("value")
      row.getInt(valueIndex)
    }

  }

  def getIntegerValueUDF() = {
    udf(getIntegerValue, IntegerType)
  }

  val getLongValue = (row: Row) => {
    if (row == null) {
      null
    } else {
      val valueIndex = row.schema.fieldIndex("value")
      row.getLong(valueIndex)
    }

  }

  def getLongValueUDF() = {
    udf(getLongValue, LongType)
  }

  val getShortValue = (row: Row) => {
    if (row == null) {
      null
    } else {
      val valueIndex = row.schema.fieldIndex("value")
      row.getShort(valueIndex)
    }

  }

  def getShortValueUDF() = {
    udf(getShortValue, ShortType)
  }

  val getNullValue = (row: Row) => {
    if (row == null) {
      null
    }
    else {
      val valueIndex = row.schema.fieldIndex("value")
      row.get(valueIndex)
    }

  }

  def getNullValueUDF() = {
    udf(getNullValue, NullType)
  }

}
