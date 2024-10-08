package com.mk.streaming

import com.mk.delta.DeltaTable
import com.mk.utils.DeltaUtils

object DeltaLakeStreamingQuickStart {
  def main(args: Array[String]): Unit = {

    print("Starting delta lake sample run")

    //DeltaTable.readGCPBigQuery()
    //DeltaTable.streamGCPBigQuery("/Users/mlln/OCI/temp/streaming/output", "/Users/mlln/OCI/temp/streaming/checkpoint")

    if (args.length == 0) {
      println("I need at least input and output path")
      return
    }


    val inputFormat = args(0)
    val inputPath = args(1)
    val deltaPath = args(2)
    val checkpoint = args(3)

    println("\n" + inputPath + ", " + deltaPath + ", " + checkpoint)
    DeltaUtils.StartDeltaStreamSink(inputFormat, inputPath, deltaPath, checkpoint)

  }
}
