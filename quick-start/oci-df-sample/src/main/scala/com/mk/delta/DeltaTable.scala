package com.mk.delta

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions.{col, current_timestamp, lit}

/*
import com.google.cloud.spark.bigquery.repackaged.com.google.cloud.bigquery.BigQuery
import com.google.cloud.spark.bigquery.repackaged.com.google.cloud.bigquery.BigQueryException
import com.google.cloud.spark.bigquery.repackaged.com.google.cloud.bigquery.BigQueryOptions
import com.google.cloud.spark.bigquery.repackaged.com.google.cloud.bigquery.TableId
import com.google.cloud.spark.bigquery.repackaged.com.google.cloud.bigquery.Table */
object DeltaTable {

  def getSparkSession(): SparkSession = {


    if ( true ) {
      val spark = SparkSession
        .builder()
        .appName("DeltaTable Simulation")
        //.config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        //.config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        //.config("spark.hadoop.fs.AbstractFileSystem.oci.impl", "com.oracle.bmc.hdfs.Bmc")
        //     .master("local[1]")
        .getOrCreate()

      spark.sparkContext.setLogLevel("ERROR")

      spark
    }
    else {
      val spark = SparkSession
        .builder()
        .appName("DeltaTable Simulation")
        //.config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        //.config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        //.config("spark.hadoop.fs.AbstractFileSystem.oci.impl", "com.oracle.bmc.hdfs.Bmc")
        .master("local[1]")
        .getOrCreate()

      spark.sparkContext.setLogLevel("ERROR")

      spark
    }

  }

  def getLocalSparkSession(): SparkSession = {
    val spark = SparkSession
      .builder()
      .appName("BigQuery Simulation")
      //.config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      //.config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      //.config("spark.hadoop.fs.AbstractFileSystem.oci.impl", "com.oracle.bmc.hdfs.Bmc")
      .config("spark.hadoop.fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
      .config("spark.hadoop.fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS")
      .config("spark.hadoop.fs.gs.auth.service.account.enable", "true")
      .master("local[1]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    spark
  }

  def csvToDelta(inputPath: String, outputPath: String) = {

    val spark = getSparkSession()

    val original_df = spark
      .read
      .format("csv")
      .option("header", "true")
      .load(inputPath)

      //val newDF = original_df.select("hack_license").withColumnRenamed("hack_license", "hack_license1")
      .withColumn("time_stamp", current_timestamp())

    original_df.write.partitionBy("vendor_id").format("delta").mode("append").save(outputPath)

  }

  def csvToParquet(inputPath: String, outputPath: String) = {

    val spark = getSparkSession()

    val original_df = spark
      .read
      .format("csv")
      .option("header", "true")
      .load(inputPath)

      .withColumn("time_stamp", current_timestamp())

    original_df.write.partitionBy("vendor_id").mode("overwrite").parquet(outputPath)

  }

  def plainCSVToParquet(inputPath: String, parquetPath: String) = {

    val spark = getSparkSession()

    val original_df = spark
      .read
      .format("csv")
      .option("header", "true")
      .load(inputPath)
      //.withColumn("par", lit(1))
      original_df.write.parquet(parquetPath)
    //original_df.write.mode(SaveMode.Append).partitionBy("par").parquet(parquetPath)

  }

  def readWriteParquet(inputPath: String, parquetPath: String) = {

    val spark = getSparkSession()

    val original_df = spark
      .read.parquet(inputPath)
      original_df.write.parquet(parquetPath)
    //original_df.write.mode(SaveMode.Append).partitionBy("par").parquet(parquetPath)

  }
/*
  def streamGCPBigQuery(outputPath: String, checkpoint: String) = {

    val spark = getLocalSparkSession()
      spark.conf.set("credentials", "ewogICJjbGllbnRfaWQiOiAiNzY0MDg2MDUxODUwLTZxcjRwNmdwaTZobjUwNnB0OGVqdXE4M2RpMzQxaHVyLmFwcHMuZ29vZ2xldXNlcmNvbnRlbnQuY29tIiwKICAiY2xpZW50X3NlY3JldCI6ICJkLUZMOTVRMTlxN01RbUZwZDdoSEQwVHkiLAogICJxdW90YV9wcm9qZWN0X2lkIjogImVtZXJhbGQtb3hpZGUtMjQxNDE3IiwKICAicmVmcmVzaF90b2tlbiI6ICIxLy8wNmtENm14eU1SSGRvQ2dZSUFSQUFHQVlTTndGLUw5SXJxM1p6cklqSGhxSF9OT2tLUUI1NktkRG0wRndjX0d2S1RKZEw2ajV1OVduaklULUJtZWYyRVdRUDhsSUJySWtFYVRRIiwKICAidHlwZSI6ICJhdXRob3JpemVkX3VzZXIiCn0=")

    spark
      .readStream.format("bigquery")
      .load("bigquery-public-data.samples.shakespeare")
      .withColumn("time_stamp_extra", current_timestamp())
      .writeStream
      .partitionBy("corpus")
      .format("delta")
      .outputMode("append")
      .option("checkpointLocation", checkpoint)
      .start(outputPath)
      .awaitTermination()

  }

  def readGCPBigQuery() = {

    val spark = getLocalSparkSession()
    spark.conf.set("credentials", "ewogICJjbGllbnRfaWQiOiAiNzY0MDg2MDUxODUwLTZxcjRwNmdwaTZobjUwNnB0OGVqdXE4M2RpMzQxaHVyLmFwcHMuZ29vZ2xldXNlcmNvbnRlbnQuY29tIiwKICAiY2xpZW50X3NlY3JldCI6ICJkLUZMOTVRMTlxN01RbUZwZDdoSEQwVHkiLAogICJxdW90YV9wcm9qZWN0X2lkIjogImVtZXJhbGQtb3hpZGUtMjQxNDE3IiwKICAicmVmcmVzaF90b2tlbiI6ICIxLy8wNmtENm14eU1SSGRvQ2dZSUFSQUFHQVlTTndGLUw5SXJxM1p6cklqSGhxSF9OT2tLUUI1NktkRG0wRndjX0d2S1RKZEw2ajV1OVduaklULUJtZWYyRVdRUDhsSUJySWtFYVRRIiwKICAidHlwZSI6ICJhdXRob3JpemVkX3VzZXIiCn0=")
   val df = spark.read
            .format("bigquery")
            .load("bigquery-public-data.samples.shakespeare")
    df.show()

    val table:Table = getTable("emerald-oxide-241417", "mksample", "sample")
    val timebeforeWrite = table.getLastModifiedTime

    val limit = 500
    df.withColumn("time_stamp", current_timestamp()).limit(limit).write
    .format("bigquery")
    .option("writeMethod", "direct")
      .mode(SaveMode.Append)
    //.option("temporaryGcsBucket","gs://mkbucket7070/checkpoint")
    .save("emerald-oxide-241417.mksample.sample")

    println ("Rows Written = " + limit)
    val table1:Table = getTable("emerald-oxide-241417", "mksample", "sample")
    val timeAfterWrite = table1.getLastModifiedTime

    println("timebeforeWrite: " + timebeforeWrite)
    println("timeAfterWrite: " + timeAfterWrite)

    spark.conf.set("viewsEnabled", "true")
    spark.conf.set("materializationDataset", "mksample")

    val sql =
      """
        SELECT *
        FROM `emerald-oxide-241417.mksample.sample` a
        WHERE TIMESTAMP_DIFF("1680108225681", "1680108225681", MINUTE) <=1
      """

    val df1 = spark.read.format("bigquery").load(sql)
    println("Total rows read from query=" + df1.count())
    println("=========")
    df1.show()

  }


  def getTable(projectId: String, datasetName: String, tableName: String): Table = {

    var table:Table = null
    try { // Initialize client that will be used to send requests. This client only needs to be created
      // once, and can be reused for multiple requests.
      val bigquery = BigQueryOptions.getDefaultInstance.getService
      val tableId = TableId.of(projectId, datasetName, tableName)
       table = bigquery.getTable(tableId)
      //System.out.println("getLastModifiedTime: " + table.getLastModifiedTime)
    } catch {
      case e: BigQueryException =>
        System.out.println("Table not retrieved. \n" + e.toString)
    }
    table
  }

  def writeStreamGCPBigQuery(tempOutputPath: String, checkpoint: String) = {

    val spark = getLocalSparkSession()
    spark.conf.set("credentials", "ewogICJjbGllbnRfaWQiOiAiNzY0MDg2MDUxODUwLTZxcjRwNmdwaTZobjUwNnB0OGVqdXE4M2RpMzQxaHVyLmFwcHMuZ29vZ2xldXNlcmNvbnRlbnQuY29tIiwKICAiY2xpZW50X3NlY3JldCI6ICJkLUZMOTVRMTlxN01RbUZwZDdoSEQwVHkiLAogICJxdW90YV9wcm9qZWN0X2lkIjogImVtZXJhbGQtb3hpZGUtMjQxNDE3IiwKICAicmVmcmVzaF90b2tlbiI6ICIxLy8wNmtENm14eU1SSGRvQ2dZSUFSQUFHQVlTTndGLUw5SXJxM1p6cklqSGhxSF9OT2tLUUI1NktkRG0wRndjX0d2S1RKZEw2ajV1OVduaklULUJtZWYyRVdRUDhsSUJySWtFYVRRIiwKICAidHlwZSI6ICJhdXRob3JpemVkX3VzZXIiCn0=")
    val df = spark.read
      .format("bigquery")
      .load("bigquery-public-data.samples.shakespeare")
    df.show()

    df.writeStream
    .format("bigquery")
    .option("temporaryGcsBucket", tempOutputPath)
    .option("checkpointLocation", checkpoint)
    .option("table", "dataset.table")

  }
*/

  def show(deltaTablePath: String) = {

    val spark = getSparkSession()
    val deltaDF = spark.read.format("delta").load(deltaTablePath);

    deltaDF.show()
  }

  def runVacuum (deltaTablePath: String) = {

    val spark = getSparkSession()
    val deltaTable = io.delta.tables.DeltaTable.forPath(spark, deltaTablePath)

    deltaTable.vacuum()
  }


  def runHistory(deltaTablePath: String) = {
    val spark = getSparkSession()

    val deltaTable = io.delta.tables.DeltaTable.forPath(spark, deltaTablePath)

    val fullHistoryDF = deltaTable.history()
    print("History for " + deltaTablePath)
    fullHistoryDF.show()
  }

  def runInSQL(parquetTablePath: String) = {
    val spark = getSparkSession()

    spark.sqlContext.sql("CONVERT TO DELTA parquet.`" + parquetTablePath + "` PARTITIONED BY (vendor_id string)");
  }

}