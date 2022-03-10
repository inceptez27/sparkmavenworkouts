package sparkstreaming
import org.apache.spark.SparkContext

import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.Seconds


object Lab02_custdata {
  
  def main(args:Array[String])=
  {
    
    val sc = new SparkContext(appName="Lab02-stream",master="local[1]")
    
    sc.setLogLevel("ERROR")
    
    val ssc = new StreamingContext(sc,Seconds(5))
    
    
    
    val dstrm = ssc.textFileStream("file:/home/hduser/stream-data")
    
    dstrm.print()
    
    val dstrm1 = dstrm.map(x => x.split(","))
    
    val dstrm2 = dstrm1.filter(x => x.length == 5)
    
    val dstrm3 = dstrm2.map(x => (x(0),x(3),x(4)))
    
    dstrm3.print()
    
    
    
    ssc.start()
    ssc.awaitTermination()
    
    
  }
  
  
}