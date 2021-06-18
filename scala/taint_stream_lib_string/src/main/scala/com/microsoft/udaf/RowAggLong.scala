package com.microsoft.udaf

import org.apache.spark.sql.{Encoder, Encoders, Row}
import org.apache.spark.sql.expressions.{Aggregator, MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types._

import scala.collection.mutable.ArrayBuffer
// import org.apache.spark.sql.functions.udaf



object RowAggLong extends UserDefinedAggregateFunction {
  def max (a: Long, b: Long) = {
    if (a >= b) {
      a
    } else {
      b
    }
  }

  def min (a: Long, b: Long) = {
    if (a <= b) {
      a
    } else {
      b
    }
  }

  def inputSchema = new StructType().add("x", LongType)
  def bufferSchema = new StructType().add("buff", LongType)
  def dataType = LongType
  def deterministic = true

  def initialize(buffer: MutableAggregationBuffer) = {
    buffer(0) = Long.MaxValue
  }

  def update(buffer: MutableAggregationBuffer, input: Row) = {
    if (!input.isNullAt(0))
      buffer(0) = min(buffer.getLong(0), input.getLong(0))
  }

  def merge(buffer1: MutableAggregationBuffer, buffer2: Row) = {
    buffer1(0) = min(buffer1.getLong(0), buffer2.getLong(0))
  }

  def evaluate(buffer: Row) = {
    buffer.getLong(0)
  }
}
