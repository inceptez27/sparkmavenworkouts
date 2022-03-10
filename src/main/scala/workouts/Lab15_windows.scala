package workouts
import org.apache.spark.sql.SparkSession
//import org.apache.spark.sql.functions.{from_csv,lit}
import org.apache.spark.sql.{Dataset,Row}

object Lab15_windows {
  
  def main(args:Array[String])=
  {
     val spark = SparkSession.builder().appName("Lab10-foreachbatch").master("local[*]").getOrCreate()
     spark.sparkContext.setLogLevel("ERROR")
     
     val tschema = spark.read.format("csv")
                      .option("header",true)
                      .option("inferschema",true)
                      .load("file:/home/hduser/stream-data1/transchema.csv").schema
  
    val df = spark.readStream
                  .format("csv")
                  .schema(tschema)
                  .option("header", false)
                  .option("maxFilesPerTrigger", 2)
                  .option("sep",",")
                  .option("includeTimestamp",true)
                  .load("file:/home/hduser/stream-data1")
  
    val options = Map("delimiter" -> ",","header"-> "false")
    
    //val df1 =  df.select(from_csv(df("value"), tschema,options).alias("txn"))
    
    val df2 = df.select("txn.txnid","txn.txndate","txn.amount","txn.category","txn.product","txn.city","txn.state","txn.payment")
   
    val df3 = df2.groupBy("city", "state")
  }
  
}