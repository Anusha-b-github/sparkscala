package org.practice.com

import org.apache.spark.sql._

trait studentTrait {
  def CorrectionOfColumns(x: DataFrame,spark:SparkSession): DataFrame{
    
  }
  def CountOfStudentsFromEachCountry(x: DataFrame,spark:SparkSession): DataFrame{
    }
//  
//  def HighestGradeInEachSubject(x: DataFrame,spark: SparkSession): DataFrame{
//    
//  }
//  def HighestGradeInEachCountry(x: DataFrame,spark:SparkSession): DataFrame{
//    
//  }
//  def TotalGradeOfEachStudent(x: DataFrame,spark:SparkSession): DataFrame{
//    
//  }
//  def CountOfStudentsBasedOnGender(x: DataFrame,spark:SparkSession): DataFrame{
//    
//  }
}
