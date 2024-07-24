package org.practice.com

import org.apache.spark.SparkConf
import org.apache.spark.sql.{SparkSession, Row}
import org.apache.spark.sql.types._

object SocialMediaData {
  
  def main(args: Array[String]): Unit = {
    
    val conf = new SparkConf().setAppName("SocialMediaData").setMaster("local[*]")
    val spark = SparkSession.builder().config(conf).getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    
    
    val socialMediaDataPath = "D:/CSVFILES/SocialMediaData.txt"
    
    val data = spark.sparkContext.textFile(socialMediaDataPath, 1)
    
    data.zipWithIndex().filter(x => x._2>0).map(x => x._1)
        
    
    
    
  }
}
