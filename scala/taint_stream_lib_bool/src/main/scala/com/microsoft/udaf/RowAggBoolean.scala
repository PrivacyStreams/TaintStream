package com.microsoft.udaf

import org.apache.spark.sql.{Encoder, Encoders, Row}
import org.apache.spark.sql.expressions.{Aggregator, MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types._

import scala.collection.mutable.ArrayBuffer
// import org.apache.spark.sql.functions.udaf

object RowAggBoolean extends UserDefinedAggregateFunction {
  def inputSchema = new StructType().add("x", BooleanType)
  def bufferSchema = new StructType().add("buff", BooleanType)
  def dataType = BooleanType
  def deterministic = true

  def initialize(buffer: MutableAggregationBuffer) = {
    buffer(0) = false
  }

  def update(buffer: MutableAggregationBuffer, input: Row) = {
    if (!input.isNullAt(0))
      buffer(0) = buffer.getBoolean(0) | input.getBoolean(0)
  }

  def merge(buffer1: MutableAggregationBuffer, buffer2: Row) = {
    buffer1(0) = buffer1.getBoolean(0) | buffer2.getBoolean(0)
  }

  def evaluate(buffer: Row) = buffer.getBoolean(0)
}

/*
 * This is for spark 3.0
object RowAggBoolean {

  object MyAverage extends Aggregator[Boolean, Boolean, Boolean] {
    // A zero value for this aggregation. Should satisfy the property that any b + zero = b
    def zero: Boolean = false
    // Combine two values to produce a new value. For performance, the function may modify `buffer`
    // and return it instead of constructing a new object
    def reduce(buffer: Boolean, data: Boolean): Boolean = {
      buffer || data
    }
    // Merge two intermediate values
    def merge(b1: Boolean, b2: Boolean): Boolean = {
      b1 || b2
    }
    // Transform the output of the reduction
    def finish(reduction: Boolean): Boolean = reduction
    // Specifies the Encoder for the intermediate value type
    def bufferEncoder: Encoder[Boolean] = Encoders.scalaBoolean
    // Specifies the Encoder for the final output value type
    def outputEncoder: Encoder[Boolean] = Encoders.scalaBoolean
  }

  def getUDAF() = udaf(MyAverage)

  def test() = println("test calling function in object")
}
 */
