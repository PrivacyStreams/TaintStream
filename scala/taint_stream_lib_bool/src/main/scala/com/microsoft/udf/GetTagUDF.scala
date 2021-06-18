package com.microsoft.udf

import org.apache.spark.sql.Row
import org.apache.spark.sql.functions.udf

object GetTagUDF {
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

  def getBooleanTagUDF() = {
    val getBooleanTag = (row: Row) => {
      recursive(row)
    }
    udf(getBooleanTag)
  }
}
