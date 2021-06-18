package com.microsoft.udf

import org.apache.spark.sql.functions.udf

object ColAggLongUDF {
  val colAgg2 = (tag_1: Long, tag_2: Long) => {
    val tmp = Array(tag_1, tag_2)
    tmp.min
  }

  val colAgg3 = (tag_1: Long, tag_2: Long, tag_3: Long) => {
    val tmp = Array(tag_1, tag_2, tag_3)
    tmp.min
  }

  val colAgg4 = (tag_1: Long, tag_2: Long, tag_3: Long, tag_4: Long) => {
    val tmp = Array(tag_1, tag_2, tag_3, tag_4)
    tmp.min
  }

  val colAgg5 = (tag_1: Long, tag_2: Long, tag_3: Long, tag_4: Long, tag_5: Long) => {
    val tmp = Array(tag_1, tag_2, tag_3, tag_4, tag_5)
    tmp.min
  }

  val colAgg6 = (tag_1: Long, tag_2: Long, tag_3: Long, tag_4: Long, tag_5: Long, tag_6: Long) => {
    val tmp = Array(tag_1, tag_2, tag_3, tag_4, tag_5, tag_6)
    tmp.min
  }

  val colAgg7 = (tag_1: Long, tag_2: Long, tag_3: Long, tag_4: Long, tag_5: Long, tag_6: Long, tag_7: Long) => {
    val tmp = Array(tag_1, tag_2, tag_3, tag_4, tag_5, tag_6, tag_7)
    tmp.min
  }

  val colAgg8 = (tag_1: Long, tag_2: Long, tag_3: Long, tag_4: Long, tag_5: Long, tag_6: Long, tag_7: Long, tag_8: Long) => {
    val tmp = Array(tag_1, tag_2, tag_3, tag_4, tag_5, tag_6, tag_7, tag_8)
    tmp.min
  }

  val colAgg9 = (tag_1: Long, tag_2: Long, tag_3: Long, tag_4: Long, tag_5: Long, tag_6: Long, tag_7: Long, tag_8: Long, tag_9: Long) => {
    val tmp = Array(tag_1, tag_2, tag_3, tag_4, tag_5, tag_6, tag_7, tag_8, tag_9)
    tmp.min
  }

  val colAgg10 = (tag_1: Long, tag_2: Long, tag_3: Long, tag_4: Long, tag_5: Long, tag_6: Long, tag_7: Long, tag_8: Long, tag_9: Long, tag_10: Long) => {
    val tmp = Array(tag_1, tag_2, tag_3, tag_4, tag_5, tag_6, tag_7, tag_8, tag_9, tag_10)
    tmp.min
  }

  val colAgg11 = (tag_1: Long, tag_2: Long, tag_3: Long, tag_4: Long, tag_5: Long, tag_6: Long, tag_7: Long, tag_8: Long, tag_9: Long, tag_10: Long, tag_11: Long) => {
    val tmp = Array(tag_1, tag_2, tag_3, tag_4, tag_5, tag_6, tag_7, tag_8, tag_9, tag_10, tag_11)
    tmp.min
  }

  def colAggLongUDF(argLen: Long) = {
    argLen match {
      case 2 => udf(colAgg2)
      case 3 => udf(colAgg3)
      case 4 => udf(colAgg4)
      case 5 => udf(colAgg5)
      case 6 => udf(colAgg6)
      case 7 => udf(colAgg7)
      case 8 => udf(colAgg8)
      case 9 => udf(colAgg9)
      case 10 => udf(colAgg10)
    }
  }
}
