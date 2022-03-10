package sparkstreaming
import org.apache.spark.SparkContext

import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.Seconds


object Lab01_c {
  
  def main(args:Array[String])=
  {
    
    val sc = new SparkContext(appName="Lab01-stream",master="local[1]")
    
    sc.setLogLevel("ERROR")
    
    val ssc = new StreamingContext(sc,Seconds(5))
    
    
    //create source connection -> process -> save/display
    
    val dstrm = ssc.textFileStream("file:/home/hduser/stream-data")
    
    dstrm.print()    
    
    
    ssc.start()
    ssc.awaitTermination()
    
    
  }
  
  
}