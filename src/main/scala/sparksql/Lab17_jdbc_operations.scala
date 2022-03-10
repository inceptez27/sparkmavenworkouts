package sparksql
import org.apache.spark.sql.SparkSession
import  com.inceptez.learn.retailproject.Utilities;


object Lab17_jdbc_operations 
{
  def main(args:Array[String])=
  {
     val spark = SparkSession.builder().appName("Lab17-jdbc").master("local").getOrCreate()
     spark.sparkContext.setLogLevel("ERROR")
     
     //jdbcconrul,username,password,table,drivers
     val df = spark.read.format("jdbc")
     .option("url","jdbc:mysql://localhost/custdb")
     .option("user","root")
     .option("password","Root123$")
     .option("dbtable","customer")
     .option("driver","com.mysql.cj.jdbc.Driver")
     .load()
     
     df.printSchema()
     
     df.show()
     
     val obj = new Utilities();
     
     println(obj.addnum(10, 20))
     
     
     
     
     
  }
  
}