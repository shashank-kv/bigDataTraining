import java.util.Calendar

import org.apache.spark.sql.Dataset
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object dataframes extends App{
    val spark = SparkSession.builder()
      .appName("Retail")
      .master("local[2]")
      .getOrCreate()
  
    val ordersDf = spark.read.csv("hdfs:///user/itv001177/project/orders").toDF("No", "Date", "CID", "Status")
  
    val customersDf = spark.read.csv("hdfs:///user/itv001177/project/customers").toDF("ID", "First", "Last", "PhoneNumber", "Email", "Address", "City", "State", "Pin")
  
    val combinedDf = customersDf.join(ordersDf, customersDf("ID") === ordersDf("CID"), "inner")
  
    //a. Get all records for customer based on name
  
    println("Enter first name")
    val first = scala.io.StdIn.readLine()
  
    println("Enter second name")
    val last = scala.io.StdIn.readLine()
  
    println("Get all records for customer based on name")
  
    val res1 = customersDf.where(s"First = '${first}' AND Last='${last}'")
  
    res1.show()
  
    //b. Count of orders based on Status and month
  
    println("Count of orders based on Status and month")
    
    val pivotDf = ordersDf.groupBy(month(col("Date")).as("month")).pivot("Status").count()
    
    pivotDf.orderBy(col("month").asc).show()
    
    //c. Count of orders based on Status and month for a customer
  
    println("Enter customer id for count of orders based on status and month")
    
    val cid = scala.io.StdIn.readLine()
  
    println("Count of orders based on Status and month for a customer")
    
    val res5 = ordersDf.where(s"CID=${cid}").groupBy(month(col("Date")).as("month"),col("Status")).count()
    
    res5.show()
  
    //d. Find count of orders based on customer and status
  
    println("Count of orders based on customer and status")
  
    val res6 = combinedDf.groupBy("CID", "State").count()
  
    res6.show()
  
    //e. Customers who placed orders
  
    println("Customers who placed orders")
  
    val res7 = combinedDf.select("ID", "First", "Last")
  
    res7.show()
  
    //f. Customers who have not placed orders
  
    println("Customers who have not placed orders")
    
    val leftDf = customersDf.join(ordersDf, customersDf("ID") === ordersDf("CID"), "left")
    
    val res8 = leftDf.where("CID = NULL")
    
    res8.show()
  
    //g. a. Top 5 highest number of orders
  
    println("Top 5 highest number of orders")
  
    val res9 = ordersDf.groupBy("CID").count().orderBy(col("count").desc).limit(5)
  
    res9.show()
  
    //g. b. Top 5 highest sum of total orders
  
    println("Top highest sum of total orders")
  
    val res10 = ordersDf.groupBy("CID").agg(sum("No").as("Total")).orderBy(col("Total").desc).limit(5)
  
    res10.show()
  
    //h. Customer who did not order in last 1 month or for long time
  
    println("Customer who did not order in last 1 month")
  
    val cur_month = Calendar.getInstance.get(Calendar.MONTH)
    
    val res12 = combinedDf.select(col("ID"), col("Date"), datediff(current_date(), col("Date")).as("diff")).where(col("diff")>30)
  
    res12.show()
  
    //i. Find last order date for all customers
  
    println("Last Order date for all customers")
  
    val res13 = ordersDf.groupBy("CID").agg(max("Date"))
  
    res13.show()
  
    //j. Find open and closed number of orders for each customer
  
    println("Number of closed orders")
  
    val res14 = combinedDf.where("Status == 'CLOSED' ").groupBy("ID").count()
  
    res14.show()
  
    println("Number of open orders")
  
    val res15 = combinedDf.where("Status != 'CLOSED' ").groupBy("ID").count()
  
    res15.show()
  
    //k. Find number of customers in every state
  
    println("Number of customers in every state")
  
    val res16 = customersDf.groupBy("State").count()
  
    res16.show()
    
    //l. Number of customers in every city
    
    println("Number of customers in every city")
    
    val res17 = customersDf.groupBy("City").count()
  
    res17.show()
    
    //m. Top 5 latest orders
    
    println("Top 5 latest orders")
    
    val res18 = ordersDf.orderBy(col("Date").desc).limit(5)
  
    res18.show()
    
    //n. Find count of orders based on city
    
    println("Orders in each city")
    
    val res19 = combinedDf.groupBy("City").count()
    
    res19.show()
    
    //Masking
    
    customersDf.withColumn("Number",col("Pin")* 25).show()
    
    customersDf.withColumn("Number",lit("***Masked***")).show()
    
    spark.stop()

}