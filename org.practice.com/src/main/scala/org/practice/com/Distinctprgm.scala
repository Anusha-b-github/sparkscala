package org.practice.com

object Distinctprgm {
  
  
  def main(args: Array[String]): Unit = {
    
    val list = List(1,2,2,3,3,4,5,6,7,8,9,9,10)
    
    var empty_list:List[Int] = List()
    
   for(x<-list){
     if(empty_list.contains(x)){
       
       //println("Dublicate")
     }else{
       empty_list = empty_list:+ x
   
     }
    empty_list
   }
    
    
    empty_list.foreach(println)
 
}
  
}