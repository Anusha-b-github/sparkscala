package org.practice.com

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object wordCountprog {
  
 def main(args: Array[String]): Unit = {
   
   
 
  val conf = new SparkConf().setAppName("word count program").setMaster("local[*]")
  val Spark = SparkSession.builder().config(conf).getOrCreate()
  
  
  val test = Spark.sparkContext.textFile("file:///C:/Users/ANUSHA/OneDrive/Desktop/SocialMediaData.txt")
  println("****************************")
  println(test.getNumPartitions)
  println("****************************")
  val word = test.flatMap(x=> x.split(" ")).map(x=> (x,1)).reduceByKey(_ + _)
  word.foreach(println)
  
  val sort_data = word.map(x=> (x._2,x._1)).sortByKey(false,1)
  sort_data.foreach(println)
  
  val filter = sort_data.filter(x => x._1 >1 && x._1 <3)
  filter.foreach(println)
  
  val filter2 = sort_data.filter(x => x._2 contains("stored"))
  filter2.foreach(println)
  
   val filter3 = sort_data.filter(x => x._2.endsWith("a") && x._2.startsWith("d"))
  filter3.foreach(println)
//sort_data.repartition(2).
//saveAsTextFile("file:///C:/Users/ANUSHA/OneDrive/Desktop/test_eclips_file_1.txt")
  println("first element:::::::::::::::::::::" + test .first())
    }
}