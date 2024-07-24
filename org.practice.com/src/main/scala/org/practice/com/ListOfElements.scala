package org.practice.com
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
object ListOfElements {
  
  
 def main(args: Array[String]): Unit = {
   
  val conf = new SparkConf().setAppName("ListOfElements").setMaster("local[*]")
    
    val spark = SparkSession.builder().config(conf).getOrCreate()

   
   val list = List(1,2,3,2,3,4,5)
   val rdd = spark.sparkContext.parallelize(list, 1)
   
   rdd.foreach(println)
 }
}