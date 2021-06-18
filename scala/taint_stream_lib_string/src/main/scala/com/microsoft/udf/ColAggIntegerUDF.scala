package com.microsoft.udf

import org.apache.spark.sql.functions.udf

object ColAggIntegerUDF {
  val colAgg2 = (tag_1: Int, tag_2: Int) => {
    val tmp = Array(tag_1, tag_2)
    tmp.min
  }

  val colAgg3 = (tag_1: Int, tag_2: Int, tag_3: Int) => {
    val tmp = Array(tag_1, tag_2, tag_3)
    tmp.min
  }

  val colAgg4 = (tag_1: Int, tag_2: Int, tag_3: Int, tag_4: Int) => {
    val tmp = Array(tag_1, tag_2, tag_3, tag_4)
    tmp.min
  }

  val colAgg5 = (tag_1: Int, tag_2: Int, tag_3: Int, tag_4: Int, tag_5: Int) => {
    val tmp = Array(tag_1, tag_2, tag_3, tag_4, tag_5)
    tmp.min
  }

  val colAgg6 = (tag_1: Int, tag_2: Int, tag_3: Int, tag_4: Int, tag_5: Int, tag_6: Int) => {
    val tmp = Array(tag_1, tag_2, tag_3, tag_4, tag_5, tag_6)
    tmp.min
  }

  val colAgg7 = (tag_1: Int, tag_2: Int, tag_3: Int, tag_4: Int, tag_5: Int, tag_6: Int, tag_7: Int) => {
    val tmp = Array(tag_1, tag_2, tag_3, tag_4, tag_5, tag_6, tag_7)
    tmp.min
  }

  val colAgg8 = (tag_1: Int, tag_2: Int, tag_3: Int, tag_4: Int, tag_5: Int, tag_6: Int, tag_7: Int, tag_8: Int) => {
    val tmp = Array(tag_1, tag_2, tag_3, tag_4, tag_5, tag_6, tag_7, tag_8)
    tmp.min
  }

  val colAgg9 = (tag_1: Int, tag_2: Int, tag_3: Int, tag_4: Int, tag_5: Int, tag_6: Int, tag_7: Int, tag_8: Int, tag_9: Int) => {
    val tmp = Array(tag_1, tag_2, tag_3, tag_4, tag_5, tag_6, tag_7, tag_8, tag_9)
    tmp.min
  }

  val colAgg10 = (tag_1: Int, tag_2: Int, tag_3: Int, tag_4: Int, tag_5: Int, tag_6: Int, tag_7: Int, tag_8: Int, tag_9: Int, tag_10: Int) => {
    val tmp = Array(tag_1, tag_2, tag_3, tag_4, tag_5, tag_6, tag_7, tag_8, tag_9, tag_10)
    tmp.min
  }

  val colAgg11 = (tag_1: Int, tag_2: Int, tag_3: Int, tag_4: Int, tag_5: Int, tag_6: Int, tag_7: Int, tag_8: Int, tag_9: Int, tag_10: Int, tag_11: Int) => {
    val tmp = Array(tag_1, tag_2, tag_3, tag_4, tag_5, tag_6, tag_7, tag_8, tag_9, tag_10, tag_11)
    tmp.min
  }

  def colAggIntegerUDF(argLen: Int) = {
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
