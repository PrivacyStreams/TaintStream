package com.microsoft.udf

import org.apache.spark.sql.functions.udf

object ColAggStringUnionUDF {
  def string2list(s: String): Set[String] = {
    val list = s.split("-")
    list.toSet
  }

  def tagMerge(a: String, b:String): String ={
    val sa = string2list(a)
    val sb = string2list(b)
    (sa ++ sb).mkString("-")
  }

  val colAgg2 = (tag_1: String, tag_2: String) => {
    var res = tagMerge(tag_1, tag_2)
    res
  }

  val colAgg3 = (tag_1: String, tag_2: String, tag_3: String) => {
    var res = tagMerge(tag_1, tag_2)
    res = tagMerge(res, tag_3)
    res
  }

  val colAgg4 = (tag_1: String, tag_2: String, tag_3: String, tag_4: String) => {
    var res = tagMerge(tag_1, tag_2)
    res = tagMerge(res, tag_3)
    res = tagMerge(res, tag_4)
    res
  }

  val colAgg5 = (tag_1: String, tag_2: String, tag_3: String, tag_4: String, tag_5: String) => {
    var res = tagMerge(tag_1, tag_2)
    res = tagMerge(res, tag_3)
    res = tagMerge(res, tag_4)
    res = tagMerge(res, tag_5)
    res
  }

  val colAgg6 = (tag_1: String, tag_2: String, tag_3: String, tag_4: String, tag_5: String, tag_6: String) => {
    var res = tagMerge(tag_1, tag_2)
    res = tagMerge(res, tag_3)
    res = tagMerge(res, tag_4)
    res = tagMerge(res, tag_5)
    res = tagMerge(res, tag_6)
    res
  }

  val colAgg7 = (tag_1: String, tag_2: String, tag_3: String, tag_4: String, tag_5: String, tag_6: String, tag_7: String) => {
    var res = tagMerge(tag_1, tag_2)
    res = tagMerge(res, tag_3)
    res = tagMerge(res, tag_4)
    res = tagMerge(res, tag_5)
    res = tagMerge(res, tag_6)
    res = tagMerge(res, tag_7)
    res
  }

  val colAgg8 = (tag_1: String, tag_2: String, tag_3: String, tag_4: String, tag_5: String, tag_6: String, tag_7: String, tag_8: String) => {
    var res = tagMerge(tag_1, tag_2)
    res = tagMerge(res, tag_3)
    res = tagMerge(res, tag_4)
    res = tagMerge(res, tag_5)
    res = tagMerge(res, tag_6)
    res = tagMerge(res, tag_7)
    res = tagMerge(res, tag_8)
    res
  }

  val colAgg9 = (tag_1: String, tag_2: String, tag_3: String, tag_4: String, tag_5: String, tag_6: String, tag_7: String, tag_8: String, tag_9: String) => {
    var res = tagMerge(tag_1, tag_2)
    res = tagMerge(res, tag_3)
    res = tagMerge(res, tag_4)
    res = tagMerge(res, tag_5)
    res = tagMerge(res, tag_6)
    res = tagMerge(res, tag_7)
    res = tagMerge(res, tag_8)
    res = tagMerge(res, tag_9)
    res
  }

  val colAgg10 = (tag_1: String, tag_2: String, tag_3: String, tag_4: String, tag_5: String, tag_6: String, tag_7: String, tag_8: String, tag_9: String, tag_10: String) => {
    var res = tagMerge(tag_1, tag_2)
    res = tagMerge(res, tag_3)
    res = tagMerge(res, tag_4)
    res = tagMerge(res, tag_5)
    res = tagMerge(res, tag_6)
    res = tagMerge(res, tag_7)
    res = tagMerge(res, tag_8)
    res = tagMerge(res, tag_9)
    res = tagMerge(res, tag_10)
    res
  }

  val colAgg11 = (tag_1: String, tag_2: String, tag_3: String, tag_4: String, tag_5: String, tag_6: String, tag_7: String, tag_8: String, tag_9: String, tag_10: String, tag_11: String) => {
    var res = tagMerge(tag_1, tag_2)
    res = tagMerge(res, tag_3)
    res = tagMerge(res, tag_4)
    res = tagMerge(res, tag_5)
    res = tagMerge(res, tag_6)
    res = tagMerge(res, tag_7)
    res = tagMerge(res, tag_8)
    res = tagMerge(res, tag_9)
    res = tagMerge(res, tag_10)
    res = tagMerge(res, tag_11)
    res
  }

  def colAggStringUnionUDF(argLen: Long) = {
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
