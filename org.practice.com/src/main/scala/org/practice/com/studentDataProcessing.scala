package org.practice.com
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object studentDataProcessing {
  
  def main(args: Array[String]): Unit = {
    
   val  conf = new SparkConf().setAppName("studentDataProcessing").setMaster("local[*]")
   val spark = SparkSession.builder().config(conf).getOrCreate()
   
   val FilePath = "D:/CSVFILES/studentdataset.csv"
   val x = spark.read.format("csv").option("header","true").option("inferSchema","true").load(FilePath)
    
    
    //read student Data file from windowFileSysytem
    
   
   val studentdata = new studentDataProcessingclass() 
    //correcting the column names
    //ethnic.group,english.grade,math.grade,sciences.grade,language.grade,portfolio.rating,coverletter.rating,refletter.rating
     val datacorrection = studentdata.CorrectionOfColumns(x,spark)
     datacorrection.show(false)
   
    
//    //find the number of students from each country 
   
     val cntofstd = studentdata.CountOfStudentsFromEachCountry(datacorrection, spark)
       cntofstd.show(50,false)
//        
//     //find the student who got highest grade in each subject and identify their respective country
//     studentdata.HighestGradeInEachSubject(x,spark)
//    
//    
//    //find the student who got highest grade (english+math+science)from each country
//    studentdata.HighestGradeInEachCountry(x,spark)
//    
//    //find the total grade of each student (english+math+science)/4
//     studentdata.TotalGradeOfEachStudent(x,spark)
//    
//    //find the number of students based on gender
//    studentdata.CountOfStudentsBasedOnGender(x,spark)

    
  }
  
}