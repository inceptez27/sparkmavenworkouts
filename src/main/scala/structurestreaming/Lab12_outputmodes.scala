package structurestreaming

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{from_csv,lit}
import org.apache.spark.sql.{Dataset,Row}

object Lab12_outputmodes {
  
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
    .option("subscribe", "customerdatatopic")
    .option("startingOffsets","latest")
    .option("group.id", "grptest")
    .load().selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
    
    
    val options = Map("delimiter" -> ",","header"-> "false")
    
    val df1 =  df.select(from_csv(df("value"), custschema,options).alias("cust"))
    
    val df2 = df1.select("cust.custid","cust.firstname","cust.lastname","cust.age","cust.profession")
    
    //default output mode is append
    /*df2.writeStream
    .format("console")
    .outputMode("append")
    .start().awaitTermination()*/
    
    
    
    val df3 = df2.groupBy("profession").count()
    
    
    //complete mode is only used for aggregated data
    /*df3.writeStream
    .format("console")
    .outputMode("complete")
    .start().awaitTermination()*/
    
    
    //outputs the updated aggregated results every time to data sink when new data arrives
    df3.writeStream
    .format("console")
    .outputMode("update")
    .start().awaitTermination()
    
    
    
  }
  
  
}
  

/*

Sink								Supported Output Modes
====          			======================
File Sink	    			Append

Kafka Sink	  			Append, Update, Complete

Foreachbath Sink		Append, Update, Complete

Console Sink				Append, Update, Complete
 
* */

