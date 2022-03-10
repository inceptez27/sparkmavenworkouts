package workouts
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{split,current_date,from_json}

object Lab07_kafkasource_filesink {
  
  def main(args:Array[String])=
  {
     val spark = SparkSession.builder().appName("Lab06-kafka").master("local[*]").getOrCreate()
     spark.sparkContext.setLogLevel("ERROR")
     val bookschema = spark.read.format("json").load("file:/home/hduser/stream-data/bookschema.json").schema
     
     val df = spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "my_topic")
    .option("group.id", "grptest")
    .load().selectExpr("CAST(timestamp AS TIMESTAMP)", "CAST(value AS STRING)")
    
    val df1 =  df.select(from_json(df("value"), bookschema).as("data"), df("timestamp"))
    
    val df2 = df1.select("data.title","data.author","data.year_written","data.edition","data.price")
    
    df2.writeStream.format("csv")
    .option("path", "file:/home/hduser/sparkstreamout")
    .option("checkpointLocation", "file:/tmp/sparkchkpoint")
    .outputMode("append") 
    .start()
    .awaitTermination()
     
  }
  
}




