package sparkcore

import org.apache.spark.SparkContext 
import org.apache.spark.sql.SparkSession


//this is the first lab excercise
object Lab01 {
  
  def main(args:Array[String])=
  {
     val lst = Array(1,2,3,4,5,6,(7,8))
    
    val tup = lst(6)
    
    
    val sc = new SparkContext(master="local",appName="Lab01")
    sc.setLogLevel("ERROR")
    
    val rdd = sc.parallelize(List(10,20,30,15,26,80))


    println(s"Sum of Numbers: ${rdd.sum()}")
    
    println(s"Max of Numbers: ${rdd.max()}")
    
    println(s"Min of Numbers: ${rdd.min()}")
    
    println(s"Max of Numbers: ${rdd.count()}")
    
    println(s"First Element: ${rdd.first()}")
    
    val lst1 = rdd.take(3)
    
    println(lst1.toList)
    
    val m = rdd.mean()

    println(s"Mean of Numbers: ${m}")

    rdd.foreach(print)
    
    print("======================")

    val rdd1 = rdd.map(x => x + 5)

    val rdd2 = rdd1.filter(x => x % 2 != 0)

    rdd2.foreach(print)
  }
  
}