# Problem Statement

###### Refinitiv Matching Engine Exercise  
Your task is to create a new matching engine for FX orders. The engine will take a CSV file of orders for a given  
currency pair and match orders together. In this example you'll be looking at USD/GBP.  
There are two types of orders, BUY and SELL orders. A BUY order is for the price in USD you'll pay for GBP, SELL  
order is the price in USD you'll sell GBP for.  
  
Each order has the following fields:  
1. Order ID  - This is a unique ID in the file which is used to track an order  
2. User Name  - This is the user name of the user making the order  
3. Order Time  - This is the time, in milliseconds since Jan 1st 1970, the order was placed  
4. Order Type  - Either BUY or SELL  
5. Quantity  - The number of currency units you want to BUY or SELL  
6. Price  - The price you wish to sell for, this is in the lowest unit of the currency, i.e. for GBP it's in pence and for USD it's cents  
  
The matching engine must do the following:  
- It should match orders when they have the same quantity  
- If an order is matched it should be closed  
- If an order is not matched it should be kept on an "order book" and wait for an order which does match  
- When matching an order to the book the order should look for the best price  
- The best price for a BUY is the lowest possible price to BUY for  
- The best price for a SELL is the highest possible price to SELL for  
- You should always use the price from the "order book" when matching orders  
- When an order has matched you should record the IDs of both orders, the price, quantity and time of the match  
- If two orders match with the same price the first order is used  
- Orders won't have the same timestamp  
The file exampleOrders.csv is some example trading data, the matches for that trading data is in outputExampleMatches.csv


# Overview

This program is developed using Scala Programming language(version:2.12.17) and spark framework(version:3.4.1) to solve above Refinitive Matching Engine problem

## Build Tool

Maven: Apache Maven is a software project management and comprehension tool. Based on the concept of a project object model (POM), Maven can manage a project's build.
Maven version : 3.8.1

## IDE
IntelliJ 2023-06

## Java

Version 1.8

## Dependency jar

org.scala-lang:scala-library:2.12.17, 
org.apache.spark:spark-sql_2.12:3.4.1

## Run Command
```
spark-submit --name "TradeExecEngine" --master local --class com.spark.trade.engine.code.SparkMainTradeEngine target/FXOrderEngine-1.0.0-SNAPSHOT-jar-with-dependencies.jar input/exampleOrders.csv output/matchOrder.csv output/orderBook.csv
```

**spark submit details :** 
1. Name of spark application 
2. Master is local 
3. scala main class path which is the entry point 
4. project jar which is maven build jar for the project

**Input Arguments details:** 
0th args: input string which contains folder name and file name
1st args: output string which is for matched order which contains output directories stucture and match order file name
2nd args: second output string which is for orderbook and contains output path and file name

# Code Walkthrough:


 1. **SparkMainTradeEngine.scala** : spark program entry point  
 2. **SparkSessionObject.scala** : defined spark session object, called in main SparkMainTradeEngine  
 3. **ReadDataFile.scala**: read the input file and return dataframe, file details passed by spark-submit argument
 4. **TradeProcessEngine.scala**: implementation of the trade order Engine logic, input parameter : dataframe
	 a. divide the dataframe for BUY and SELL

 	 b. inner join both the dataframe on Quantity

 	 c. select the columns :                  
             Max(buy.order,sell.order) as orderID1,
    
             Min(buy.order,sell.order) as orderID2,
    
             Max(buy.orderTime,sell.oderTime) as orderTime,
    
	     buy.Quantity as Quantity,
    
	     when(sell.OrderID < buy.OrderID) then sell.price else buy.price as price
    	 
    d. return matchedOrder dataframe
    
    f. collect OrderId from matchedOrder dataframe
    	
    g. select record from input dataframe where OrderId not in collect OrderId from matchedOrder dataframe
    	 
    h. return unmatchedOrder

6. **WriteDataFile.scala** : write each of the output dataframe(matchedOrder and unmatchedOrderbook) without header and remove the file directory and part files, output file detils from spark submit arguments
## Input file

```
1,Steve,1623239770,SELL,72,1550
2,Ben,1623239771,BUY,8,148
3,Steve,1623239772,SELL,24,6612
4,Kim,1623239773,SELL,98,435
5,Sarah,1623239774,BUY,72,5039
6,Ben,1623239775,SELL,75,6356
7,Kim,1623239776,BUY,38,7957
8,Alex,1623239777,BUY,51,218
9,Jennifer,1623239778,SELL,29,204
10,Alicia,1623239779,BUY,89,7596
11,Alex,1623239780,BUY,70,2351
12,James,1623239781,SELL,89,4352
13,Sarah,1623239782,SELL,98,8302
14,Alicia,1623239783,BUY,56,8771
15,Alex,1623239784,BUY,83,737
16,Andrew,1623239785,SELL,15,61
17,Steve,1623239786,BUY,62,4381
18,Ben,1623239787,BUY,33,5843
19,Alicia,1623239788,BUY,20,5255
20,James,1623239789,SELL,68,4260
```

## Output file ( MatchOrder)

```
5,1,1623239774,72,1550
12,10,1623239781,89,7596
```

## Output file ( UnmatchedOrder/OrderBook)


```
2,Ben,1623239771,BUY,8,148
3,Steve,1623239772,SELL,24,6612
4,Kim,1623239773,SELL,98,435
6,Ben,1623239775,SELL,75,6356
7,Kim,1623239776,BUY,38,7957
8,Alex,1623239777,BUY,51,218
9,Jennifer,1623239778,SELL,29,204
11,Alex,1623239780,BUY,70,2351
13,Sarah,1623239782,SELL,98,8302
14,Alicia,1623239783,BUY,56,8771
15,Alex,1623239784,BUY,83,737
16,Andrew,1623239785,SELL,15,61
17,Steve,1623239786,BUY,62,4381
18,Ben,1623239787,BUY,33,5843
19,Alicia,1623239788,BUY,20,5255
20,James,1623239789,SELL,68,4260
```

