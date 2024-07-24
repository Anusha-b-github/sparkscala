package org.practice.com
import org.apache.spark.sql.functions._
import org.apache.spark.sql._

class studentDataProcessingclass extends studentTrait   {
  
   
  
    def CorrectionOfColumns(x:DataFrame,spark:SparkSession):DataFrame = {
       //ethnic.group,english.grade,math.grade,sciences.grade,language.grade,portfolio.rating,coverletter.rating,refletter.rating

      var df = x
      df= df.withColumnRenamed("ethnic.group", "ethnic_group")
                .withColumnRenamed("english.grade", "english_grade")
                .withColumnRenamed("math.grade", "math_grade")
                .withColumnRenamed("sciences.grade", "sciences_grade")
                .withColumnRenamed("language.grade", "language_grade")
                .withColumnRenamed("portfolio.rating", "portfolio_rating")
                .withColumnRenamed("coverletter.rating", "coverletter_rating")
                .withColumnRenamed("refletter.rating", "refletter.rating")
                .drop("ethnic.group","english.grade","sciences.grade","language.grade","portfolio.rating","coverletter.rating","refletter.rating")
df
     
        //find the number of students from each country 

     }
   def CountOfStudentsFromEachCountry(x:DataFrame,spark:SparkSession): DataFrame={
     var df = x
     df = df.groupBy("nationality").agg(count("nationality").alias("CountOfStudentsFromEachCountry"))
           .select(col("nationality").alias("country"), col("CountOfStudentsFromEachCountry"))
     df
      
   }
//    def HighestGradeInEachSubject(x:DataFrame,spark:SparkSession):DataFrame={
//      
//    }
//    def HighestGradeInEachCountry(x:DataFrame,spark:SparkSession): DataFrame={
//      
//    }
//    def TotalGradeOfEachStudent(x:DataFrame,spark:SparkSession): DataFrame ={
//      
//    }
//    def CountOfStudentsBasedOnGender(x:DataFrame,spark:SparkSession): DataFrame={
//      
//    }
      
  
}