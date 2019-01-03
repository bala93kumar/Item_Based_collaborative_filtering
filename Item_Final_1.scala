package completed

import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.recommendation.ALS
// $example off$
import org.apache.spark.sql.SparkSession
import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.sql.functions._
import org.apache.spark.rdd._
import org.apache.spark.sql.expressions.Window

import org.apache.spark.sql.Row


object Item_Final_1 {
  
  case class Rating(userId: Int, profile: Int, rating: Int)
   
  def parseRating( str: String) : Rating = {
    val fields = str.split(" ")
      Rating(fields(0).toInt, fields(1).toInt , 1)
  }
   
  def main(args: Array[String]) {
    
    
    Logger.getLogger("org").setLevel(Level.ERROR)
    

    val ss = SparkSession
      .builder
      .appName("Association_2")   //.master("local[*]")
      .getOrCreate()
      
    import ss.implicits._
   

   
   val  in =  ss.read.textFile(args(0)).map(parseRating).toDF()
   
   val als = new ALS()
    .setMaxIter(5)
    .setRegParam(0.01)
    .setUserCol("userId")
    .setItemCol("profile")
    .setRatingCol("rating")
    
    
  val model = als.fit(in) 
  
  
  val userRecs = model.recommendForAllUsers(10)
  
 // userRecs.show()
  
  
  val a =  userRecs.select($"userId", explode($"recommendations")).select($"userId",$"col.profile")
  
  
  val ratings_2 = in.select("userId" , "profile") 
  
  val out = a.except(ratings_2)
  
   out.show(100)
   
  
  out.write.text(args(0))
   //a.show()
   
   
   
   
   ss.stop()
  
}
  
}