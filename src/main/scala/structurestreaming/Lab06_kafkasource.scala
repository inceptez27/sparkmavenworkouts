package structurestreaming

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{split,current_date,from_csv,col}

object Lab06_kafkasource {
  
  def main(args:Array[String])=
  {
     val spark = SparkSession.builder().appName("Lab06-kafka").master("local[*]").getOrCreate()
     spark.sparkContext.setLogLevel("ERROR")
     val custschema = spark.read.format("csv")
                      .option("header",true)
                      .option("inferschema",true)
                      .load("file:/home/hduser/stream-data/schemadata.csv")
                      .schema
                      
                      
     val df = spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "customer_topic")
    .option("startingOffsets","latest")
    .load().selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
    
    
    
    val options = Map("delimiter" -> ",","header"-> "false")
    
    val df1 =  df.select(from_csv(col("value"), custschema,options).alias("cust"))
    
    val df2 = df1.select("cust.custid","cust.firstname","cust.lastname","cust.age","cust.profession")
    
    
    df2.writeStream.format("console").start().awaitTermination()
    
     
  }
  
}

/*


option("startingoffsets", "latest")  - wait only for the new messages in the topic. 

option("startingoffsets", "earliest")  - allows rewind for missed alerts.  

{"topicA":{"0":23,"1":-1},"topicB":{"0":-1}}

 (-1 is used for the 'latest', -2 - for the earliest) 




For file sink, it supports only append


*/

