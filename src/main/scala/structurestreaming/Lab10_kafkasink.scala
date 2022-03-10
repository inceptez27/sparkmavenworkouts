package structurestreaming

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{split,current_date,to_json,from_csv,col}

object Lab10_kafkasink {
  
  def main(args:Array[String])=
  {
     val spark = SparkSession.builder().appName("Lab10-kafkasink").master("local[*]").getOrCreate()
     spark.sparkContext.setLogLevel("ERROR")
     
                      
                      
     val df = spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "customertopic1")
    .option("startingOffsets","latest")
    .load().selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
    
    
    val df1 =  df.select(split(col("value"),",").alias("cust"))
    
    val df3 = df1.withColumn("value", to_json(df1("cust")))
    
    df3.writeStream.format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("topic", "customerkafkatopic")
    .option("checkpointLocation", "file:/tmp/sparkstreamchkpoint")
    .start()
    .awaitTermination()
    
  }
  
}

/*


The Dataframe being written to Kafka should have the following columns in schema:

Column -	            Type
=====                 ====    
key (optional)	      string or binary
value (required)	    string or binary
headers (optional)	  array
topic (*optional)	    string
partition (optional)	int



*/

