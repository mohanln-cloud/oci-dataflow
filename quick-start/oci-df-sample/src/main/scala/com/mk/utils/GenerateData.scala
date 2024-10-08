package com.mk.utils

object GenerateData {

  def main(args: Array[String]): Unit = {

    print("Starting delta lake sample run")

    if (args.length == 0) {
      println("I need at least input and output path")
    }

    val inputFormat = args(0)
    val inputPath = args(1)

    val outputFormat = args(2)
    val outputPath = args(3)
    val sleepTimeInSec = args(4)
    val totalRuns = args(5).toInt

    println("\n" + inputFormat + ":" +inputPath +  ", " + outputFormat + ":" + outputPath)
    var count = 0

     while(count < totalRuns) {

       println("Wakeup " + count + " -- " + System.currentTimeMillis())
       //DeltaUtils.generateData(inputPath, outputForamt, outputPath + s"-$count") with new data directory
       DeltaUtils.generateData(inputFormat, inputPath, outputFormat, outputPath)

       Thread.sleep(1000 * sleepTimeInSec.toInt)
       count = count + 1
     }
  }
}
