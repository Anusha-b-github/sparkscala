package org.practice.com
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.length
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions.substring

object StudentDataExplode {
  def main(args: Array[String]): Unit = {
    
    val conf = new SparkConf().setAppName("StudentDataExplode").setMaster("local[*]")
    val spark = SparkSession.builder().config(conf).getOrCreate()
    
    val data = spark.read.format("csv")
                         .option("header", "true")
                         .option("inferSchema", "true")
                         .load("file:///D:/CSVFILES/StudentDataExplode.csv")
                         
    //data.show(false)
                         
              val data1 = data.withColumn("length", length(col("marks")) - 1)
              data1.show(false)
              
//              val data2 = data1.withColumn("substring", substring(col("marks"), 2, col("length")))
//                  data2.show(false)
              
    
  }
}