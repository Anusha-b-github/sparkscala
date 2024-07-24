package org.practice.com

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{StructType, StructField, StringType, IntegerType}
import org.apache.spark.sql.functions.col

object dropmalFormed {
  
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("dropmalformed").setMaster("local[*]")
    
    val spark = SparkSession.builder().config(conf).getOrCreate()
    
    spark.conf.set("spark.sql.csv.parse.columnPruning.enabled","flase")
    
    // Defining schema of the data DROPMALFORMED MODE
    val Schema = StructType(Array(
      StructField("Emp_id", IntegerType, nullable = true),
      StructField("Emp_name", StringType, nullable = true),
      StructField("designation", StringType, nullable = true),
      StructField("salary", IntegerType, nullable = true),
      StructField("DOB", StringType, nullable = true)
    ))

    val csvFile = spark.read
      .format("csv")
      .schema(Schema)
      .option("header", "true")
      .option("inferSchema", "true")
      .option("mode", "DROPMALFORMED")
      .load("file:///D:/emp_data.csv")
        
    csvFile.show()
    
       
// Define the schema, including the _corrupted_records field
   val Schema1 = StructType(Array(
  StructField("Emp_id", IntegerType, true),
  StructField("Emp_name", StringType, true),
  StructField("designation", StringType, true),
  StructField("salary", IntegerType, true),
  StructField("DOB", StringType, true),
  StructField("_corrupt_record", StringType, true)
      
    ))

     // Reading the CSV file with permissive mode
val csvFile1 = spark.read
  .format("csv")
  .schema(Schema1)
  .option("delimiter", ",")
  .option("header", "true")
  .option("mode", "PERMISSIVE")
  .option("columnNameOfCorruptRecord", "_corrupt_record")
  .load("file:///D:/emp_data.csv")

// Show the DataFrame
csvFile1.show(false)

    csvFile1.filter(col("_corrupt_record").isNull).show(false)
    
    
   
  }
}
