package com.spark.trade.engine.code

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, greatest, least, when}

object TradeProcessEngine {


  def tradeProcess(inputOrdersDF:DataFrame): (DataFrame,DataFrame) = {

    //Separating  OrderType into two different dataFrame
    val buyOrdersDF = inputOrdersDF.filter(col("OrderType") === "BUY")
    val sellOrdersDF = inputOrdersDF.filter(col("OrderType") === "SELL")

    // Join Buy and sell dataframe on Quantity to get only matching record
    val combineOrderDF = buyOrdersDF.as("buy")
      .join(sellOrdersDF.as("sell"),
        buyOrdersDF("Quantity") === sellOrdersDF("Quantity"), "inner")

    // Find the best price for BUY/SELL
    val getMatchedRecord = combineOrderDF.select(
      greatest(col("sell.OrderID"), col("buy.OrderID")).as("Order1"),
      least(col("sell.OrderID"), col("buy.OrderID")).as("Order2"),
      when(col("sell.orderTime") > col("buy.orderTime"),
        col("sell.orderTime")).otherwise(col("buy.orderTime")).alias("OrderTime"),
      col("buy.Quantity"),
      when(col("sell.OrderID") < col("buy.OrderID"), col("sell.price"))
        .otherwise(col("buy.price")).alias("Price"))
    getMatchedRecord.show()

    // get OrderID Seq of matched record to remove matched data from orderlist
    val getMatchedOrderId = getMatchedRecord.select(col("Order1")).union(getMatchedRecord.select(col("Order2")))
      .collect.map(_.getInt(0)).toSeq

    // select only unmatched record from order list
    val unmatchedOrderBook = inputOrdersDF.filter(!inputOrdersDF.col("orderId").isin(getMatchedOrderId: _*))
    unmatchedOrderBook.show()

    (getMatchedRecord,unmatchedOrderBook)
  }

}
