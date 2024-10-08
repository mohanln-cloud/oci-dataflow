package com.mk.logger

import com.mk.delta.DeltaTable
import org.apache.logging.log4j.LogManager

object LoggerTestApp {
  def main(args: Array[String]): Unit = {

    print("Starting sample run\n")
    //System.setProperty("log4j2.configurationFile", "com/mk/logger/log4j2.xml")
    //System.setProperty("logFilename", "logs/myApp.log")
    givenLoggerWithDefaultConfig_whenLogToConsole_thanOK()

  }

  @throws[Exception]
  def givenLoggerWithDefaultConfig_whenLogToConsole_thanOK(): Unit = {
    val logger = LogManager.getLogger(getClass)
    val e = new RuntimeException("RuntimeException occurred!!!")
    logger.info("\nThis is a simple message at INFO level. " + "It will be hidden.")
    logger.error("\nThis is a simple message at ERROR level. " + "This is the minimum visible level.", e)
  }
}
