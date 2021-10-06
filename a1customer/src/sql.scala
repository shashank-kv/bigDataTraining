import java.util.Calendar

import org.apache.spark.sql.Dataset
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._


object sql extends App{
    val spark = SparkSession.builder()
      .appName("Retail")
      .master("local[2]")
      .getOrCreate()
  
    val ordersDf = spark.read.csv("hdfs:///user/itv001177/project/orders").toDF("No", "Date", "CID", "Status")
  
    val customersDf = spark.read.csv("hdfs:///user/itv001177/project/customers").toDF("ID", "First", "Last", "PhoneNumber", "Email", "Address", "City", "State", "Pin")
  
    val combinedDf = customersDf.join(ordersDf, customersDf("ID") === ordersDf("CID"), "inner")
  
    ordersDf.createOrReplaceTempView("orders")
  
    customersDf.createOrReplaceTempView("customers")
  
    //a. Get all records for customer based on name
  
    println("Enter first name")
    val first = scala.io.StdIn.readLine()
  
    println("Enter second name")
    val last = scala.io.StdIn.readLine()
  
    println("Get all records for customer based on name")
  
    spark.sql(s"select * from customers where First='${first}' AND Last='${last}'").show()
  
    //b. Count of orders based on Status and month
  
    println("Count of orders based on Status and month")
  
    spark.sql("select count(No),Status,month(Date) from orders group by Status,month(Date)").show();
    
    //c. Count of orders based on Status and month for a customer
  
    println("Enter customer id for count of orders based on status and month")
     
    val cid = scala.io.StdIn.readLine()
  
    println("Count of orders based on Status and month for a customer")
    
    spark.sql(s"select count(No),Status,month(Date) from orders where CID=${cid} group by Status,month(Date)").show();
  
    //d. Find count of orders based on customer and status
  
    println("Count of orders based on customer and status")
  
    spark.sql("select count(No),customers.ID,Status from customers inner join orders where customers.ID=orders.CID group by customers.ID,Status").show()
  
    //e. Customers who placed orders
  
    println("Customers who placed orders")
  
    spark.sql("select First,Last,ID from customers inner join orders where customers.ID=orders.CID").show()
  
    //f. Customers who have not placed orders
    
    println("Customers who have not placed orders")
    
    spark.sql("select First,Last,ID from customers left join orders on customers.ID=orders.CID where orders.CID=NULL").show()
     
    //g. a. Top 5 highest number of orders
    
    println("Top 5 highest number of orders")
    
    spark.sql("select * from orders order by No desc limit 5").show();
    
    //g. b. Top 5 highest sum of total orders
    
    println("Top highest sum of total orders")
    
    spark.sql("select count(No), CID from orders group by CID order by count(No) desc limit 5").show();
  
    //h. Customer who did not order in last 1 month or for long time
    
    val cur_month = Calendar.getInstance.get(Calendar.MONTH)
    
    println("Customer who did not order in last 1 month")
    
    spark.sql("select CID FROM orders where (month(Date) - month(current_date)) >= 1").show()
    
    //i. Find last order date for all customers
    
    println("Last Order date for all customers")
    
    spark.sql("select max(Date) from orders group by CID").show()
    
    //j. Find open and closed number of orders for each customer
    
    println("Number of closed orders")
    
    spark.sql("select count(No),customers.ID from customers inner join orders where customers.ID=orders.CID AND Status='CLOSED' group by customers.ID").show()
  
    println("Number of open orders")
    
    spark.sql("select count(No),customers.ID from customers inner join orders where customers.ID=orders.CID AND Status!='CLOSED' group by customers.ID").show()
    
    //k. Find number of customers in every state
    
    println("Number of customers in every state")
    
    spark.sql("select count(ID),State from customers group by State").show()
    
    //l. Number of customers in every city
    
    println("Number of customers in every city")
    
    spark.sql("select count(ID),City from customers group by City").show()
    
    //m. Top 5 latest orders
    
    println("Top 5 latest orders")
    
    spark.sql("select * from orders order by Date desc limit 5").show();
    
    //n. Find count of orders based on city
    
    println("Orders in each city")
    
    spark.sql("select count(No), City from customers inner join orders where customers.ID=orders.CID group by City").show()
    
    spark.stop()
    
}