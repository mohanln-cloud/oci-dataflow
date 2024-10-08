package com.mk.delta

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
    val deltaPath = args(1)
    val loopIt = args(2).toInt

    println("\n" + inputPath + ", " + deltaPath)

    for (w <- 0 until loopIt) {
      println("Running loop " + w);

      //DeltaTable.plainCSVToParquet(inputPath, parquetPath + "/" + w)
      //DeltaTable.plainCSVToParquet(inputPath, parquetPath + "/" + w)
      DeltaTable.csvToDelta(inputPath, deltaPath)
      DeltaTable.show(deltaPath)
      DeltaTable.runVacuum(deltaPath)
      //DeltaTable.readWriteParquet(inputPath, parquetPath + "/" + w)
      //DeltaTable.readGCPBigQuery()
    }

  }
}
