package com.mk.bigquery

import com.mk.delta.DeltaTable

object BigQuerySample {
  def main(args: Array[String]): Unit = {

    print("Starting Big query sample run")

    if (args.length == 0) {
      println("I need at least input and output path")
      return
    }

    val inputPath = args(0)
    val outputPath = args(1)
    val checkpoint = args(2)

    println("\n" + inputPath + ", " + checkpoint)

//    DeltaTable.readGCPBigQuery()
    //DeltaTable.streamGCPBigQuery(outputPath, checkpoint)

  }
}
