package com.microsoft.udaf

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types._
import org.apache.spark
import com.microsoft.udf.GetTagUDF.getStringTagUDF
import com.microsoft.udf.GetValueUDF.{getLongValueUDF, getStringValueUDF}
import com.microsoft.udf.PackUDF.{packLongUDF, packStringUDF}
import com.microsoft.udf.ColAggBooleanUDF.colAggBooleanUDF
import org.apache.spark.sql.functions.col
import com.microsoft.udaf.RowAggStringJoin


object sparkTest {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .config("spark.master", "local")
      .appName("Spark SQL user-defined DataFrames aggregation test")
      .getOrCreate()


    var df = spark.read.json("src/main/resources/input.json")
    df.printSchema()
    df.show()
    // test get tag
    df.agg(RowAggStringJoin(col("people.email.address.tag")), RowAggStringJoin(col("people.email.content.tag")))
      .show()
  }
}
