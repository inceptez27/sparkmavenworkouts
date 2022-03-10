package structurestreaming

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{split,current_date,from_csv,col}

object Lab09_kafkasource_filesink {
  
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
    .option("subscribe", "customerkafkatopic")
    .option("startingOffsets","earliest")
    .option("group.id", "grptest")
    .load().selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
    
    
    val options = Map("delimiter" -> ",","header"-> "false")
    
    val df1 =  df.select(from_csv(col("value"), custschema,options).alias("cust"))
    
    val df2 = df1.select("cust.custid","cust.firstname","cust.lastname","cust.age","cust.profession")
    
    //df2.writeStream.format("console").option("truncate",false).start().awaitTermination()
    
    df2.writeStream.format("csv")
    .option("path", "file:/home/hduser/sparkcuststreamout")
    .option("checkpointLocation", "file:/tmp/sparkchkpoint1")
    .start()
    .awaitTermination()
    
     
  }
  
}

/*


Checkpointing is an important concept as it allows recovery from failures and also where last left the processing.
offsets are stored in the directory called checkpoint.
Also in this directory information about the output file writes is  stored. 
Checkpoints used to store intermediate information for fault  tolerance 



For file sink, it supports only append

read -> process -> write

Semantics:

Exactly-once 
Atmost-once
Atleast-once



*/

