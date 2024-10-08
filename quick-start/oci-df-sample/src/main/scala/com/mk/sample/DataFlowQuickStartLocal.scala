package com.mk.sample

import com.mk.delta.DeltaTable
import org.apache.logging.log4j.LogManager

object DataFlowQuickStartLocal {
  def main(args: Array[String]): Unit = {

    print("Starting delta lake sample run")

    //DeltaTable.readGCPBigQuery()
    //DeltaTable.streamGCPBigQuery("/Users/mlln/OCI/temp/streaming/output", "/Users/mlln/OCI/temp/streaming/checkpoint")

    if (args.length == 0) {
      println("I need at least input and output path")
      return
    }

    val inputPath = args(0)
    val parquetPath = args(1) + "/" + System.currentTimeMillis()
    val loopIt = args(2).toInt

    println("\n" + inputPath + ", " + parquetPath)

    for (w <- 0 until loopIt) {
      println("Running loop " + w);

      //DeltaTable.plainCSVToParquet(inputPath, parquetPath + "/" + w)
      DeltaTable.plainCSVToParquet(inputPath, parquetPath + "/" + w)
      //DeltaTable.readWriteParquet(inputPath, parquetPath + "/" + w)
      //DeltaTable.readGCPBigQuery()
    }

  }

  @throws[Exception]
  def givenLoggerWithDefaultConfig_whenLogToConsole_thanOK(): Unit = {
    val logger = LogManager.getLogger(getClass)
    val e = new RuntimeException("\nThis is only a test!")
    logger.info("\nThis is a simple message at INFO level. " + "It will be hidden.")
    logger.error("\nThis is a simple message at ERROR level. " + "This is the minimum visible level.", e)
  }
}
