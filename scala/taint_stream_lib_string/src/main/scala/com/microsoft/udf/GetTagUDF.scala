package com.microsoft.udf

import org.apache.spark.sql.Row
import org.apache.spark.sql.functions.udf

object GetTagUDF {
  def min (a: Int, b: Int) = {
    if (a <= b) {
      a
    } else {
      b
    }
  }

  def minLong (a: Long, b: Long) = {
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
    (sa ++ sb).mkString("-")
  }

  def recursive(row: Row):Boolean = {
    val fields = row.schema.fieldNames
    if (fields.contains("value") && fields.contains("tag") && fields.length == 2) {
      val tagIndex = row.schema.fieldIndex("tag")
      row.getBoolean(tagIndex)
    } else {
      var ret = false
      for (filed <- row.schema) {
        val tag = recursive(row.getAs[Row](filed.name))
        ret = ret | tag
      }
      ret
    }
  }

  def recursiveInt(row: Row):Int = {
    val fields = row.schema.fieldNames
    if (fields.contains("value") && fields.contains("tag") && fields.length == 2) {
      val tagIndex = row.schema.fieldIndex("tag")
      row.getInt(tagIndex)
    } else {
      var ret = Int.MaxValue
      for (filed <- row.schema) {
        val tag = recursiveInt(row.getAs[Row](filed.name))
        ret = min(ret, tag)
      }
      ret
    }
  }

  def recursiveLong(row: Row):Long = {
    val fields = row.schema.fieldNames
    if (fields.contains("value") && fields.contains("tag") && fields.length == 2) {
      val tagIndex = row.schema.fieldIndex("tag")
      try{
        row.getLong(tagIndex)
      } catch {
        case ex: NullPointerException => {
          Long.MaxValue
        }
      }
    } else {
      var ret = Long.MaxValue
      for (filed <- row.schema) {
        val tag = recursiveLong(row.getAs[Row](filed.name))
        ret = minLong(ret, tag)
      }
      ret
    }
  }

  def recursiveString(row: Row):String = {
    val fields = row.schema.fieldNames
    if (fields.contains("value") && fields.contains("tag") && fields.length == 2) {
      val tagIndex = row.schema.fieldIndex("tag")
      try{
        row.getString(tagIndex)
      } catch {
        case ex: NullPointerException => {
          ""
        }
      }
    } else {
      var ret = Set[String]()
      for (filed <- row.schema) {
        val tag = recursiveString(row.getAs[Row](filed.name))
        ret = ret ++ string2list(tag)
      }
      ret.mkString("-")
    }
  }

  def recursiveStringJoin(row: Row):String = {
    val fields = row.schema.fieldNames
    if (fields.contains("value") && fields.contains("tag") && fields.length == 2) {
      val tagIndex = row.schema.fieldIndex("tag")
      try{
        row.getString(tagIndex)
      } catch {
        case ex: NullPointerException => {
          "ycx-lyc-wq"
        }
      }
    } else {
      var ret = Set[String]("ycx", "lyc", "wq")
      for (filed <- row.schema) {
        val tag = recursiveStringJoin(row.getAs[Row](filed.name))
        ret = ret & string2list(tag)
      }
      ret.mkString("-")
    }
  }

  def getBooleanTagUDF() = {
    val getBooleanTag = (row: Row) => {
      recursive(row)
    }
    udf(getBooleanTag)
  }

  def getIntegerTagUDF() = {
    val getIntegerTag = (row: Row) => {
      recursiveInt(row)
    }
    udf(getIntegerTag)
  }

  def getLongTagUDF() = {
    val getLongTag = (row: Row) => {
      recursiveLong(row)
    }
    udf(getLongTag)
  }

  def getStringTagUDF() = {
    val getStringTag = (row: Row) => {
      recursiveString(row)
    }
    udf(getStringTag)
  }

  def getStringJoinTagUDF() = {
    val getStringTag = (row: Row) => {
      recursiveStringJoin(row)
    }
    udf(getStringTag)
  }
}
