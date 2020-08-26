package Execution

import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import FileReader.FileReader
import caseclass.{CovidIndiaCases, IndividualDetails}
import org.apache.spark.sql.functions.when
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions.{avg, coalesce, datediff, lit, to_date, unix_timestamp}

object CovidProcessor {

  def startExecution(sparkSession: SparkSession,sc: SparkContext):Unit={

    println("Starting Processing!")
    var covid_India=FileReader.loadFileToDS("covid_19_india.csv",sparkSession,sc)
    covidTotal_ByStates(covid_India)

    var individual_details= FileReader.loadIndividualDetailsToDS("IndividualDetails.csv",sparkSession,sc);
    individual_details.show(100);

      var age_details=FileReader.loadAgeGroupToDS("AgeGroupDetails.csv",sparkSession,sc);
        age_details.show(100);
//    var averageAge= getAverage(individual_details);
//      print("The average age is: "+ averageAge);
    var df= findCategory(individual_details);
    df.show(1000);
  }

//  def getAverage(ds: Dataset[IndividualDetails]): Unit= {
//   var ds2= ds.withColumn("age_int",col("age").cast("Int"))
////      .filter("age_int is not null")
////        .agg(avg(col("age_int")))
//
//    val expr=ds2.columns.map(c=>coalesce(col("age_int"),avg(col("age_int"))))
//    ds2.select(expr: _*).show(1000)
//
//  }

  def covidTotal_ByStates(ds:Dataset[CovidIndiaCases]): Unit = {

    print("Showing Statewise Total Count!");
    var df=ds.groupBy("State_UnionTerritory")
      .sum("confirmed")
      .select("State_UnionTerritory","sum(confirmed)")
        .orderBy("sum(confirmed)")

    df.show(100);
  }

  def findCategory(ds:Dataset[IndividualDetails]): DataFrame ={

    print("Finding Category of Spread!");

    var df=ds.withColumn("category", when(col("notes").contains("travel"),"Travel History")
    .when(col("notes").contains("contact"),"Came in contact with infected Person")
    .when(col("notes").contains("relative"),"Local Transmission")
    .otherwise("unknown"))

    return df.where(col("category").contains("travel"));
  }
}
