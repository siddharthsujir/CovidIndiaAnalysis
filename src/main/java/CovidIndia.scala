import Execution.CovidProcessor
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession

object CovidIndia extends App {


  override def main(args: Array[String]): Unit = {
    print("Hello Spark!");

    var conf= new SparkConf()
      .setMaster("local[*]")
      .setAppName("CovidIndia")

    var sc=SparkContext.getOrCreate(conf);
    var sparkSession=SparkSession.builder()
      .master("local[*]")
      .config(conf)
      .getOrCreate();

    CovidProcessor.startExecution(sparkSession,sc);
  }

}
