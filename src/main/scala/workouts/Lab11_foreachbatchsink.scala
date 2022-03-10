package workouts

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{from_csv,lit}
import org.apache.spark.sql.{Dataset,Row}

object Lab11_foreachbatchsink {
  
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
    .option("subscribe", "customerkafkatopic")
    .option("startingOffsets","earliest")
    .option("group.id", "grptest")
    .load().selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
    
    
    val options = Map("delimiter" -> ",","header"-> "false")
    
    val df1 =  df.select(from_csv(df("value"), custschema,options).alias("cust"))
    
    val df2 = df1.select("cust.custid","cust.firstname","cust.lastname","cust.age","cust.profession")
    
    df2.writeStream
    .foreachBatch(saveToMySql)
    .outputMode("append")
    .start()
    .awaitTermination()
    
   
    
  }
  
  val saveToMySql = (df: Dataset[Row], batchId: Long) => 
    {
      val df1 = df.withColumn("Batch", lit(batchId))
      
      df1.write.format("jdbc")
        .option("url", "jdbc:mysql://localhost/custdb")
        .option("dbtable", "tblstudentmarks")
        .option("user", "root")
        .option("password", "Root123$")
        .mode("append")
        .save()
      print("written into mysql")
    }
}
  

/*

Foreachbatch: allows you to specify a function that is executed on the output data of every micro-batch of the streaming query.




*/

