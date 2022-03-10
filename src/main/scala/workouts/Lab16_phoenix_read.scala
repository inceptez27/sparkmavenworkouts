package workouts
import org.apache.spark.sql.SparkSession

object Lab16_phoenix_read {
  
  def main(args:Array[String]):Unit=
  {
    val spark = SparkSession.builder().appName("Lab10-foreachbatch").master("local[*]").getOrCreate()
     spark.sparkContext.setLogLevel("ERROR")
     
    val df = spark.read.format("org.apache.phoenix.spark")
    .option("table", "CUSTOMER").option("zkUrl", "localhost:2181").load()
    
    df.show()
  }
}