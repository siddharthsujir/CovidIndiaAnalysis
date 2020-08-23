package FileReader

import caseclass.{AgeGroupDetails, CovidIndiaCases, IndividualDetails}
import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

object FileReader {


  def loadFile(filename:String,sparkSession: SparkSession,sc: SparkContext ): DataFrame = {

    var df= sparkSession.read.format("csv")
      .option("header","true")
      .option("inferSchema","true")
      .option("nullValue","-")
      .option("path",filename)
      .load();

  return df;
  }

  def loadFileToDS(filename: String, sparkSession: SparkSession, sc: SparkContext): Dataset[CovidIndiaCases] ={

    import sparkSession.sqlContext.implicits._;
    return sparkSession.read.format("csv")
        .option("header",true)
        .option("nullValue","-")
        .option("inferSchema","true")
        .option("path",filename)
        .load().as[CovidIndiaCases];
  }

  def loadIndividualDetailsToDS(filename: String, sparkSession: SparkSession,sc: SparkContext): Dataset[IndividualDetails] = {
    import sparkSession.sqlContext.implicits._;

    return sparkSession.read.format("csv")
        .option("inferSchema","true")
        .option("header","true")
        .option("path",filename)
        .option("nullValue","-")
        .load()
        .as[IndividualDetails];
  }

  def loadAgeGroupToDS(filename: String, sparkSession: SparkSession,sc: SparkContext): Dataset[AgeGroupDetails] =
  {
    import sparkSession.sqlContext.implicits._;

    return sparkSession.read.format("csv")
        .option("inferSchema","true")
        .option("header","true")
        .option("path",filename)
        .option("nullValue","-")
        .load()
        .as[AgeGroupDetails];
  }

}
