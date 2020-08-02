package Execution

import org.apache.spark.SparkContext
import org.apache.spark.sql.{Dataset, SparkSession}
import FileReader.FileReader
import caseclass.CovidIndiaCases

object CovidProcessor {

  def startExecution(sparkSession: SparkSession,sc: SparkContext):Unit={

    println("Starting Processing!")
    var covid_India=FileReader.loadFileToDS("covid_19_india.csv",sparkSession,sc)
    covidTotal_ByStates(covid_India)
  }

  def covidTotal_ByStates(ds:Dataset[CovidIndiaCases]): Unit = {

    print("Showing Statewise Total Count!");
    var df=ds.groupBy("State_UnionTerritory")
      .sum("confirmed")
      .select("State_UnionTerritory","sum(confirmed)")
        .orderBy("sum(confirmed)")

    df.show(100);
  }
}
