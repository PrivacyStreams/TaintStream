package com.microsoft.udf

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types._
import org.apache.spark
import com.microsoft.udf.GetTagUDF.getBooleanTagUDF
import com.microsoft.udf.GetValueUDF.{getLongValueUDF, getStringValueUDF}
import com.microsoft.udf.PackUDF.{packLongUDF, packStringUDF}
import com.microsoft.udf.ColAggBooleanUDF.colAggBooleanUDF
import org.apache.spark.sql.functions.col

object SparkTest {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .config("spark.master", "local")
      .appName("Spark SQL user-defined DataFrames aggregation example")
      .getOrCreate()


    var df = spark.read.json("src/main/resources/input.json")
    df.printSchema()
    df.show()
    // test get tag
    val testUDF = getBooleanTagUDF()
    df.select(
      testUDF(col("people.email")),
      testUDF(col("people")),
      testUDF(col("people.email.content"))
    ).show()

    // test get value
    val testUDF_2 = colAggBooleanUDF(2)
    df.select(
      // testUDF(col("people.email")),
      // testUDF(col("people")),
      testUDF_2(col("people.email.content.tag"), col("people.email.address.tag"))
    ).show()
  }
}
