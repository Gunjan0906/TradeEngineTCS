package com.spark.trade.engine.code

import org.apache.spark.sql.{DataFrame}

object SparkMainTradeEngine {
  def main(args: Array[String]): Unit = {

    // get spark session object
    val spark = SparkSessionObject.getSparkSession("TradeMatchEngine")

    //Spark .Success file generation false in output folder
    spark.conf.set("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false")

    // read data from csv file and schema from case class Order
    val inputOrdersDF = ReadDataFile.readData(spark, args(0).toString)

    inputOrdersDF.show()

    //Trade process Engine
    val (getMatchedRecord:DataFrame,unmatchedOrderBook:DataFrame) = TradeProcessEngine.tradeProcess(inputOrdersDF)

    // Writing the matched and updated Orderbook without header defiled in sample output
    WriteDataFile.writeDataFile(spark, getMatchedRecord, args(1).toString)
    WriteDataFile.writeDataFile(spark, unmatchedOrderBook, args(2).toString)


  }
}