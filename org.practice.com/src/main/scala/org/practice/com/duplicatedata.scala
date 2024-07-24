package org.practice.com
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession



object duplicatedata {
  def main(args: Array[String]): Unit = {
    
    
    val conf = new SparkConf().setAppName("duplicatedata").setMaster("local[*]")
    
    val spark = SparkSession.builder().config(conf).getOrCreate()
    
    spark.conf.set("spark.sql.csv.parse.columnPruning.enabled","flase")
    
    val csvFile = spark.read
      .format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load("file:///D:/emp_data1.csv")
        
    //csvFile.show()
    //csvFile.dropDuplicates().show(false)
      csvFile.dropDuplicates("Emp_id","salary").show(false)
    println("======================================")
        println("======================================")
    //csvFile.distinct().show(false)
    
  }
}