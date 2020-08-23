package Execution

import org.apache.spark.SparkContext
import org.apache.spark.sql.{Dataset, SparkSession}
import FileReader.FileReader
import caseclass.{CovidIndiaCases, IndividualDetails}
import org.apache.spark.sql.functions.when
import org.apache.spark.sql.functions.col

object CovidProcessor {

  def startExecution(sparkSession: SparkSession,sc: SparkContext):Unit={

    println("Starting Processing!")
    var covid_India=FileReader.loadFileToDS("covid_19_india.csv",sparkSession,sc)
    covidTotal_ByStates(covid_India)

    var individual_details= FileReader.loadIndividualDetailsToDS("IndividualDetails.csv",sparkSession,sc);
    individual_details.show(100);

    findCategory(individual_details);
  }

  def covidTotal_ByStates(ds:Dataset[CovidIndiaCases]): Unit = {

    print("Showing Statewise Total Count!");
    var df=ds.groupBy("State_UnionTerritory")
      .sum("confirmed")
      .select("State_UnionTerritory","sum(confirmed)")
        .orderBy("sum(confirmed)")

    df.show(100);
  }

  def findCategory(ds:Dataset[IndividualDetails]): Unit ={

    print("Finding Category of Spread!");

    var df=ds.withColumn("category", when(col("notes").contains("travel"),"Travel History")
    .when(col("notes").contains("contact"),"Came in contact with infected Person")
    .when(col("notes").contains("relative"),"Local Transmission")
    .otherwise("unknown"))

    df.show();
  }
}
