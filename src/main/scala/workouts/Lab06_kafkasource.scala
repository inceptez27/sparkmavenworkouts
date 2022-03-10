package workouts
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
    .option("subscribe", "transtopic")
    .option("group.id", "grptest")
    .load().selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
    
    val options = Map("delimiter" -> ",")
    
    val df1 =  df.select(from_csv(col("value"), custschema,options).alias("cust"))
    
    val df2 = df1.withColumn("current_dt", current_date())
    
    df2.writeStream.format("console").start().awaitTermination()
    
     
  }
  
}