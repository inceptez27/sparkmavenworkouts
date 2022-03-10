package structurestreaming

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{from_csv,lit,split,col}
import org.apache.spark.sql.{Dataset,Row}

object Lab11_foreachbatchsink {
  
  def main(args:Array[String])=
  {
     val spark = SparkSession.builder().appName("Lab10-foreachbatch").master("local[*]").getOrCreate()
     spark.sparkContext.setLogLevel("ERROR")
     
                      
     val df = spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "customertopic1")
    .option("startingOffsets","latest")
    .option("group.id", "grptest")
    .load().selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
    
    val df1 =  df.select(split(df("value"), ",").alias("cust"))
    
    val df2 = df1.select(col("cust").getItem(0).alias("custid"),col("cust").getItem(1).alias("fname"),col("cust").getItem(3).alias("age"))
    
    df2.writeStream
    .foreachBatch(saveToMySql)
    .outputMode("append")
    .start()
    .awaitTermination()
    
   
    
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
        .option("dbtable", "tblcustomerdata_raw1")
        .option("user", "root")
        .option("password", "Root123$")
        .mode("append")
        .save()
        
      println("written into mysql")
    }
}
  

/*

Foreachbatch: allows you to specify a function that is executed on the output data of every micro-batch of the streaming query.




*/

