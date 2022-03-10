package structurestreaming

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{split,current_date,from_json,col}

object Lab08_kafkasource_json {
  
  def main(args:Array[String])=
  {
     val spark = SparkSession.builder().appName("Lab06-kafka").master("local[*]").getOrCreate()
     spark.sparkContext.setLogLevel("ERROR")
      val bookschema = spark.read.format("json").load("file:/home/hduser/stream-data/bookschema.json").schema
     
     val df = spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "customer_json")
    .load().selectExpr("CAST(timestamp AS TIMESTAMP)", "CAST(value AS STRING)")
    
    val df1 =  df.select(from_json(df("value"), bookschema).as("data"), df("timestamp"))
    
    val df2 = df1.select("data.title","data.author","data.year_written","data.edition","data.price")
    
    df2.writeStream.format("console").option("truncate",false).start().awaitTermination()
    
     
  }
  
}

/*


option("startingoffsets", "latest")  - wait only for the new messages in the topic. 

option("startingoffsets", "earliest")  - allows rewind for missed alerts.  

{"topicA":{"0":23,"1":-1},"topicB":{"0":-1}}

 (-1 is used for the 'latest', -2 - for the earliest) 


Checkpointing is an important concept as it allows recovery from failures and also where last left the processing.
offsets are stored in the directory called checkpoint.
Also in this directory information about the output file writes is  stored. 
Checkpoints used to store intermediate information for fault  tolerance 



For file sink, it supports only append


*/

