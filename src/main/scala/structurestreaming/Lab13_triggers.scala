package structurestreaming
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{from_csv,lit}
import org.apache.spark.sql.{Dataset,Row}
import org.apache.spark.sql.streaming.Trigger


object Lab12_triggers {
  def main(args:Array[String])=
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
    .option("startingOffsets","""{"customerdatatopic":{"0":0}}""")
    .option("group.id", "grptest")
    .load().selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
    
    
    val options = Map("delimiter" -> ",","header"-> "false")
    
    val df1 =  df.select(from_csv(df("value"), custschema,options).alias("cust"))
    
    val df2 = df1.select("cust.custid","cust.firstname","cust.lastname","cust.age","cust.profession")
    
    /*df2.writeStream
    .format("console")
    .outputMode("append")
    .trigger(Trigger.ProcessingTime("10 seconds"))
    .start().awaitTermination()
    */
    /*  df2.writeStream
    .format("console")
    .outputMode("append")
    .trigger(Trigger.Once())
    .start().awaitTermination()*/
    
    df2.writeStream.foreachBatch(saveToMySql).trigger(Trigger.Once()).start().awaitTermination()
    println("Written into mysql")
    
  }
  
  val saveToMySql = (df: Dataset[Row], batchId: Long) => 
    {
      val df1 = df.withColumn("Batch", lit(batchId))
      
     /* df1.write.format("org.apache.phoenix.spark")
      .mode("overwrite")
      .option("table", "TRANSDATA")
      .option("zkUrl", "localhost:2181")
      .save()*/
      
      df1.write.format("jdbc")
        .option("url", "jdbc:mysql://localhost/custdb")
        .option("dbtable", "tblcustomerdata_once")
        .option("user", "root")
        .option("password", "Root123$")
        .mode("append")
        .save()
        
      println("written into mysql")
    }
}


//test
/*

Triggers:
=========

Default: Executes a micro-batch as soon as the previous finishes
Fixed interval micro-batches: Specifies the interval when the micro-batches will execute. For example, 1 minute , 30 seconds or 1 hour etc
One-time micro-batch: Executes only one micro-batch to process all available data and then stops.




If micro-batch completes within the [given] interval, then the engine will wait until the interval is over before kicking off the next micro-batch.

If the previous micro-batch takes longer than the interval to complete (i.e. if an interval boundary is missed), then the next micro-batch will start as soon as the previous one completes (i.e., it will not wait for the next interval boundary).

If no new data is available, then no micro-batch will be kicked off.

*/