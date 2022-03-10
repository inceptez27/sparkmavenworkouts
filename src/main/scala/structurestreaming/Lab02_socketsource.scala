package structurestreaming

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Lab02_socketsource {
  
  def main(args:Array[String])=
  {
     val spark = SparkSession.builder().appName("Lab02-socket").master("local[*]").getOrCreate()
     spark.sparkContext.setLogLevel("ERROR")
     
     val df = spark.readStream.format("socket").option("host", "localhost").option("port",9999).load()
     
     val df1 = df.withColumn("current_dt", current_date())
    
     df1.writeStream.format("console").option("truncate", false).start().awaitTermination()
    
  }
}


/*
 
Read from Input sources -> Transformation -> Write into Sink 
 
Input Sources:
=============

Socket: This data source will listen to the specified socket and ingest any data into Spark Streaming.

Sink Types:
========== 

Console sink: Displays the content of the DataFrame to console



*/