package structurestreaming

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._


object Lab04_schemaread {
  
  def main(args:Array[String])=
  {
     val spark = SparkSession.builder().appName("Lab03-file").master("local[*]").getOrCreate()
     spark.sparkContext.setLogLevel("ERROR")
     
     val custschema = spark.read.format("csv")
                      .option("header",true)
                      .option("inferschema",true)
                      .load("file:/home/hduser/stream-data/schemadata.csv")
                      .schema
     
     val df = spark.readStream
                  .format("csv")
                  .schema(custschema)
                  .option("header", false)
                  .option("maxFilesPerTrigger", 2)
                  .load("file:/home/hduser/stream-data")
    
    val df1 = df.select("custid","age","profession")              
                  
    val df2 = df1.withColumn("current_dt", current_date())
   
    df2.writeStream.format("console").start().awaitTermination()
    
  }
  
}

/*
 
 Read from Input sources -> Transformation -> Write into Sink 
 
Input Sources:
=============

Rate (for Testing): It will automatically generate data including 2 columns timestamp and value . This is generally used for testing purposes. 
Socket: This data source will listen to the specified socket and ingest any data into Spark Streaming.
File: This will listen to a particular directory as streaming data. It supports file formats like CSV, JSON, ORC, and Parquet.
Kafka: This will read data from Apache Kafka® and is compatible with Kafka broker versions 0.10.0 or higher 


Sink Types:
========== 

Console sink: Displays the content of the DataFrame to console
File sink: Stores the contents of a DataFrame in a file within a directory. Supported file formats are csv, json, orc, and parquet.
Kafka sink: Publishes data to a Kafka topic
Foreachbatch: allows you to specify a function that is executed on the output data of every micro-batch of the streaming query.



*/