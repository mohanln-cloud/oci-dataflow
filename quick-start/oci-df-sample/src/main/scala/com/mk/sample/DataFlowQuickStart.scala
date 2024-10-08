package com.mk.sample

import com.mk.delta.DeltaTable

object DataFlowQuickStart {
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
}
