package com.mk.streaming

import com.mk.utils.DeltaUtils.getSparkSession
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

object SparkStreamingFromDirectory {

  def main(args: Array[String]): Unit = {

    if (args.length == 0) {
      println("I need at least input and output path")
      return
    }

    val inputPath = args(0)
    val parquetPath = args(1) + "/" + System.currentTimeMillis()
    val checkpointPath = args(2)

    println("\n" + inputPath + ", " + parquetPath)

    val spark:SparkSession = getSparkSession()

    val schema = StructType(
      List(
        StructField("RecordNumber", IntegerType, true),
        StructField("Zipcode", StringType, true),
        StructField("ZipCodeType", StringType, true),
        StructField("City", StringType, true),
        StructField("State", StringType, true),
        StructField("LocationType", StringType, true),
        StructField("Lat", StringType, true),
        StructField("Long", StringType, true),
        StructField("Xaxis", StringType, true),
        StructField("Yaxis", StringType, true),
        StructField("Zaxis", StringType, true),
        StructField("WorldRegion", StringType, true),
        StructField("Country", StringType, true),
        StructField("LocationText", StringType, true),
        StructField("Location", StringType, true),
        StructField("Decommisioned", StringType, true)
      )
    )

    val df = spark.readStream
      .schema(schema)
      .json(inputPath)

    df.printSchema()

    df.writeStream
      .format("delta")
      .outputMode("append")
      .partitionBy("Zipcode")
      .option("checkpointLocation", checkpointPath)
      .option("truncate",false)
      .option("newRows",30)
      .start(parquetPath)
      .awaitTermination()
  }
}
