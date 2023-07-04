package com.spark.trade.engine.code

import org.apache.spark.sql.SparkSession

object SparkSessionObject {
  def getSparkSession(AppName : String ) : SparkSession ={
     SparkSession.builder()
      .appName(AppName)
      .master("local").getOrCreate()
  }
}
