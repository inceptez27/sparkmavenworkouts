package sparkstreaming
import org.apache.spark.SparkContext
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.Seconds
import org.apache.spark.sql.SparkSession

object Lab03_socketstream 
{
  
  def main(args:Array[String])=
  {
    //nc -lk 9999
    
    val spark = SparkSession.builder().appName("Lab03-stream").master("local[2]").getOrCreate()
    
    val sc = spark.sparkContext
    
    sc.setLogLevel("ERROR")
    
    val ssc = new StreamingContext(sc,Seconds(5))
     
    
    val dstrm = ssc.socketTextStream("localhost",9999)
    
    val dstrm1 = dstrm.map(x => x.split(","))
    
    val dstrm2 = dstrm1.filter(x => x.length == 5)
    
    val dstrm3 = dstrm2.map(x => (x(0),x(3),x(4)))
    
    dstrm3.foreachRDD(rdd => {
          
      if(!rdd.isEmpty())
      {
         import spark.implicits._
         
         val df = rdd.toDF("custid","age","prof")
         
         df.show()
        
      }
    
    })   
    
    dstrm3.print()
    
    ssc.start()
    ssc.awaitTermination()
    
  }
  
}