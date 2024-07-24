package org.practice.com

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object RDDoperations {
  
  
  def main(args: Array[String]): Unit = {
    
    
    val conf = new SparkConf().setAppName("RDD operations").setMaster("local[*]")
    
    val spark =SparkSession.builder().config(conf).getOrCreate()
    
    
    val test = List(1,2,3,4,5,65,5,5,6,8,9,7)
    println("::::::::::::::::::take operation::::::::::::::::::::::::")
    test.take(4).foreach(x=>println(x))
    
   val rdd = spark.sparkContext.parallelize(test, 1)
  rdd.foreach(println)
  
  val rdd1 = rdd.distinct()
  println("distinct values **********************")
 rdd1.foreach(println) 
  
   val  rdd2 = rdd1.isEmpty()
   println(rdd2)
   
   val rdd3 = rdd.filter(x=> x != 5 && x != 65)
  rdd3.foreach(println)
  
  val testmap = test.map(x=> x*3)
  testmap.foreach(println)
  
  val even_odd = test.map(x=> if (x%2==0) ("EVEN",x) else ("odd",x) )
  even_odd.foreach(println)
  
  val example = test.map(x=> if (x>50) x else (x+"::less than 50"))
  
  example.foreach(println)
  
  val sub = test.map(x=> x-2)
  sub.foreach(println)
  
  
  
  val rdd_union = sub.union(test)
  rdd_union.foreach(println)
  
  
  
      }
}