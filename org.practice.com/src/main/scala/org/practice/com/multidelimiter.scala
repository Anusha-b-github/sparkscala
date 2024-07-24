package org.practice.com
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.split
import org.apache.spark.sql.functions.column


object multidelimiter {
  def main(args: Array[String]): Unit = {
    
     val conf = new SparkConf().setAppName("duplicatedata").setMaster("local[*]")
    
    val spark = SparkSession.builder().config(conf).getOrCreate()
    
    val csvFile = spark.read
      .format("csv")
      .option("delimiter", "$")
      .option("header", "true")
      .option("inferSchema", "true")
      .load("file:///D:/CSVFILES/emp_data(MD).csv")
    
 //csvFile.show(false)
 
 var dm = csvFile.withColumn("salary", split(csvFile.col("salary,DOB"),"\\,")(0).cast("Integer"))
    
 dm = dm.withColumn("DOB", split(csvFile.col("salary,DOB"),"\\,")(1))
 dm = dm.drop("salary,DOB")
   dm.show(false)
   dm.printSchema()
 
  }
  
  
}

