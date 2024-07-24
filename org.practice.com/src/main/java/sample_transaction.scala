package org.practice.com
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Row



object sample_transaction {
  def main(args: Array[String]): Unit = {
    
    val conf = new SparkConf().setAppName("sample_transaction").setMaster("local[*]")
    
    val spark = SparkSession.builder().config(conf).getOrCreate()
    
     val transaction_data = "D:/sample_transaction.txt"
     val costumer_data = "D:/cust_info.txt"
   
     val transaction_data_csv = spark.sparkContext.textFile(transaction_data, 1)
     val costumer_data_csv = spark.sparkContext.textFile(costumer_data, 1)
    
     val split_data = transaction_data_csv.map(x=> x.split(" ")).map(x =>Row(x:_*))
     
  }
  
  
}