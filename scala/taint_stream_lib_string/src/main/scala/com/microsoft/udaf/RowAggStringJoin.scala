package com.microsoft.udaf

import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{StructType, _}
// import org.apache.spark.sql.functions.udaf


object RowAggStringJoin extends UserDefinedAggregateFunction {
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

  def string2list(s: String): Set[String] = {
    val list = s.split("-")
    list.toSet
  }

  def tagMergeUnion(a: String, b:String): String ={
    val sa = string2list(a)
    val sb = string2list(b)
    println(sa, sb)
    (sa & sb).mkString("-")
  }

  def inputSchema = new StructType().add("x", StringType)
  def bufferSchema = new StructType().add("buff", StringType)
  def dataType = StringType
  def deterministic = true

  def initialize(buffer: MutableAggregationBuffer) = {
    buffer(0) = "ycx-lyc-wq"
  }

  def update(buffer: MutableAggregationBuffer, input: Row) = {
    if (!input.isNullAt(0)) {
      val sa = string2list(buffer.getString(0))
      val sb = string2list(input.getString(0))
      buffer(0) = (sa & sb).mkString("-")
    }
  }

  def merge(buffer1: MutableAggregationBuffer, buffer2: Row) = {
    val sa = string2list(buffer1.getString(0))
    val sb = string2list(buffer2.getString(0))
    buffer1(0) = (sa & sb).mkString("-")
  }

  def evaluate(buffer: Row) = {
    buffer.getString(0)
  }
}
