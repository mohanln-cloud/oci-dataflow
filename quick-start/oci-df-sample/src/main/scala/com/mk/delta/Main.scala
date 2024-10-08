package com.mk.delta

import com.mk.iceberg.IcebergTable

object Main {

  def main(args: Array[String]): Unit = {

    print("Starting delta lake sample run")

    if (args.length == 0) {
      println("I need at least input and output path")
    }

    val inputPath = args(0)
    val deltaPath = args(1)
    val parquetPath = args(2)

    println("\n" + inputPath +  ", " + deltaPath)

//    IcebergTable.runIcebergSample(inputPath, parquetPath)

    DeltaTable.csvToDelta(inputPath, deltaPath)
    DeltaTable.csvToParquet(inputPath,parquetPath)
    DeltaTable.show(deltaPath)

    DeltaTable.runVacuum(deltaPath)

    DeltaTable.runInSQL(parquetPath)
    DeltaTable.show(parquetPath)
    DeltaTable.runHistory(deltaPath)

  }

}