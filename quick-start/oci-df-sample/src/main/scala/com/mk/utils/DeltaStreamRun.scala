package com.mk.utils

object DeltaStreamRun {

  def main(args: Array[String]): Unit = {

    print("Starting delta lake sample run")

    if (args.length == 0) {
      println("I need at least input and output path")
    }

    val inputFormat = args(0)
    val inputPath = args(1)
    val deltaPath = args(2)
    val checkpoint = args(3)

    println("\n" + inputPath +  ", " + deltaPath +  ", " + checkpoint)
    DeltaUtils.StartDeltaStreamSink(inputFormat, inputPath, deltaPath, checkpoint)

  }
}
