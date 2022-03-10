package workouts

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{split,current_date,to_json,from_csv,col}

object Lab10_kafkasink {
  
  def main(args:Array[String])=
  {
     val spark = SparkSession.builder().appName("Lab10-kafkasink").master("local[*]").getOrCreate()
     spark.sparkContext.setLogLevel("ERROR")
     
     val custschema = spark.read.format("csv")
                      .option("header",true)
                      .option("inferschema",true)
                      .load("file:/home/hduser/stream-data/schemadata.csv")
                      .schema
                      
                      
     val df = spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "customerkafkatopic")
    .option("startingOffsets","earliest")
    .option("group.id", "grptest")
    .load().selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
    
    
    val options = Map("delimiter" -> ",","header"-> "false")
    
    val df1 =  df.select(from_csv(col("value"), custschema,options).alias("cust"))
    
    val df2 = df1.select("cust.custid","cust.firstname","cust.lastname","cust.age","cust.profession")
    
    val df3 = df2.withColumn("value", to_json(df2("cust")))
    
    //kafka sink
    //It pushes data from the value column in the dataframe
    
    df3.writeStream.format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("topic", "spark_topic")
     .option("checkpointLocation", "file:/tmp/sparkchkpoint")
    .outputMode("append")
    .start()
    .awaitTermination()
    
  }
  
}

/*


The Dataframe being written to Kafka should have the following columns in schema:

Column -	            Type
key (optional)	      string or binary
value (required)	    string or binary
headers (optional)	  array
topic (*optional)	    string
partition (optional)	int



*/

