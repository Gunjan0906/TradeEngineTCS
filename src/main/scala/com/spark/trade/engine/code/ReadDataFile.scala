package com.spark.trade.engine.code

import org.apache.spark.sql.{DataFrame, Encoders, SparkSession}

case class OrderBook(orderId: Int, user: String, orderTime: Long, OrderType: String, quantity: Int, price: Int)

object ReadDataFile {

  def readData(spark:SparkSession, input: String) : DataFrame = {
    spark.read
      .format("org.apache.spark.sql.execution.datasources.csv.CSVFileFormat")
      .schema(Encoders.product[OrderBook].schema)
      .load(input)
  }

}
