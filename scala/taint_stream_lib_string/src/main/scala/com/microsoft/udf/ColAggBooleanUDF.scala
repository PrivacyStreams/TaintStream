package com.microsoft.udf

import org.apache.spark.sql.Row
import org.apache.spark.sql.functions.udf

object ColAggBooleanUDF {
  val colAgg2 = (tag_1: Boolean, tag_2: Boolean) => {
    tag_1 | tag_2
  }

  val colAgg3 = (tag_1: Boolean, tag_2: Boolean, tag_3: Boolean) => {
    tag_1 | tag_2 | tag_3
  }

  val colAgg4 = (tag_1: Boolean, tag_2: Boolean, tag_3: Boolean, tag_4: Boolean) => {
    tag_1 | tag_2 | tag_3 | tag_4
  }

  val colAgg5 = (tag_1: Boolean, tag_2: Boolean, tag_3: Boolean, tag_4: Boolean, tag_5: Boolean) => {
    tag_1 | tag_2 | tag_3 | tag_4 | tag_5
  }

  val colAgg6 = (tag_1: Boolean, tag_2: Boolean, tag_3: Boolean, tag_4: Boolean, tag_5: Boolean, tag_6: Boolean) => {
    tag_1 | tag_2 | tag_3 | tag_4 | tag_5 | tag_6
  }

  val colAgg7 = (tag_1: Boolean, tag_2: Boolean, tag_3: Boolean, tag_4: Boolean, tag_5: Boolean, tag_6: Boolean, tag_7: Boolean) => {
    tag_1 | tag_2 | tag_3 | tag_4 | tag_5 | tag_6 | tag_7
  }

  val colAgg8 = (tag_1: Boolean, tag_2: Boolean, tag_3: Boolean, tag_4: Boolean, tag_5: Boolean, tag_6: Boolean, tag_7: Boolean, tag_8: Boolean) => {
    tag_1 | tag_2 | tag_3 | tag_4 | tag_5 | tag_6 | tag_7 | tag_8
  }

  val colAgg9 = (tag_1: Boolean, tag_2: Boolean, tag_3: Boolean, tag_4: Boolean, tag_5: Boolean, tag_6: Boolean, tag_7: Boolean, tag_8: Boolean, tag_9: Boolean) => {
    tag_1 | tag_2 | tag_3 | tag_4 | tag_5 | tag_6 | tag_7 | tag_8 | tag_9
  }

  val colAgg10 = (tag_1: Boolean, tag_2: Boolean, tag_3: Boolean, tag_4: Boolean, tag_5: Boolean, tag_6: Boolean, tag_7: Boolean, tag_8: Boolean, tag_9: Boolean, tag_10: Boolean) => {
    tag_1 | tag_2 | tag_3 | tag_4 | tag_5 | tag_6 | tag_7 | tag_8 | tag_9 | tag_10
  }

  val colAgg11 = (tag_1: Boolean, tag_2: Boolean, tag_3: Boolean, tag_4: Boolean, tag_5: Boolean, tag_6: Boolean, tag_7: Boolean, tag_8: Boolean, tag_9: Boolean, tag_10: Boolean, tag_11: Boolean) => {
    tag_1 | tag_2 | tag_3 | tag_4 | tag_5 | tag_6 | tag_7 | tag_8 | tag_9 | tag_10 | tag_11
  }

  def colAggBooleanUDF(argLen: Int) = {
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
