//	package org.practice.com
//	import org.apache.spark.SparkConf
//	import org.apache.spark.sql.SparkSession
//	import org.apache.spark.sql.Row
//
//	object socialMediaData {
//	  def main(args: Array[String]): Unit = {
//		
//	  
//	  
//	  val conf = new SparkConf().setAppName("socialMediaData").setMaster("local[*]")
//	  val spark = SparkSession.builder().config(conf).getOrCreate()
//	  
//	 val  filepath = "D:/CSVFILES/SocialMediaData.txt"
//	  val data = spark.sparkContext.textFile(filepath)
//	   val schema =  data.take(1).flatMap(x =>x.split(",")).map(x=> x.trim()).toList
//	   //schema.foreach(println)
//	   val filepath1 = data.flatMap(x => x.split(",")).map(x=> Row(x.trim():_*))
//	  filepath1.foreach(println)
//		
//		
//	}
//	}