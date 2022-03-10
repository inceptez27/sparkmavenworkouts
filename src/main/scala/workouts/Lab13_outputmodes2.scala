package workouts

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{from_csv,lit}
import org.apache.spark.sql.{Dataset,Row}

object Lab13_outputmodes2 {
  
  def main(args:Array[String]):Unit=
  {
     val spark = SparkSession.builder().appName("Lab10-foreachbatch").master("local[*]").getOrCreate()
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
    
    val df1 =  df.select(from_csv(df("value"), custschema,options).alias("cust"))
    
    val df2 = df1.select("cust.custid","cust.firstname","cust.lastname","cust.age","cust.profession")
    
    //default output mode is append
    val sink1 = df2.writeStream
    .format("console")
    .outputMode("append")
    .start()
    
    val df3 = df2.groupBy("profession").count()
    
    
    //complete mode is only used for aggregated data
    val sink2 = df3.writeStream
    .format("console")
    .outputMode("complete")
    .start()
    
    
    //outputs the updated aggregated results every time to data sink when new data arrives
    val sink3 = df3.writeStream
    .format("console")
    .outputMode("update")
    .start()
    
    sink1.awaitTermination()
    sink2.awaitTermination()
    sink3.awaitTermination()
    
    
    
  }
  
  
}
  

/*

Sink								Supported Output Modes
====          			======================
File Sink	    			Append

Kafka Sink	  			Append, Update, Complete

Foreachbath Sink		Append, Update, Complete

Console Sink					Append, Update, Complete
 
* */

