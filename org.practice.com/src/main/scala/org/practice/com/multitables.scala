package org.practice.com
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.functions.col
import java.io.File
import java.util.Properties

object multitables {
  
  
  def main(args: Array[String]): Unit = {
    
   val conf = new SparkConf().setAppName("duplicatedata").setMaster("local[*]")
    
    val spark = SparkSession.builder().config(conf).getOrCreate()
    
    val srcpath = "D:/multiplefiles"
    val targetpath = "D:/output"
    
    val path = new File(srcpath)
      
   val hostname = "jdbc:postgresql://localhost:5432/test"
   val username = "postgres"
   val password = "admin"
  
   
   
   val connection = new Properties()
   connection.setProperty("url",hostname)
   connection.setProperty("Driver", "org.postgresql.Driver")
   connection.setProperty("user", username)
   connection.setProperty("password", password)
   val tablename = "multiplefiles"
   
  // val pc = spark.read.jdbc(hostname, tablename, connection)
  // pc.select(col("*")).show(false)
   
   
      path.listFiles().foreach(file => {
         
         if(file.isFile()){
           
           //println("NAME OF THE FILE IS :::::::"+file.getName)
           var filename = file.getName
           var srcFile = spark.read
                              .format("csv")
                              .option("header", true)
                              .option("inferSchema", true)
                              .option("mode", "DROPMALFORMED")
                              .load(srcpath + "/"+file.getName)
                              
           srcFile=srcFile.withColumn("fileName", lit(filename).substr(0,filename.length()-4 ))
           //srcFile.write.mode("append").format("csv").save(targetpath)
           srcFile.write.mode("append").jdbc(hostname, tablename, connection)
           
         }
         
         else{
           
           println("NAME OF THE folder IS :::::::"+file.getName)
         }
         
         
       }
 
       )   
  
  }  
  
  
  
  
  
}