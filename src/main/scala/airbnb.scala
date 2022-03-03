import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._

class airbnb {

  def readCSV(): Unit ={

    val df = sparksess()
            .read
            .option("header",true)
      .option("multiLine", "true")
      .csv("C:/Users/Dishu/Downloads/listings.csv")

    df.show(10)

    val filterData = df
      .filter(col("scrape_id").isNotNull)
        .groupBy(col("host_id"))
      .agg(count("*").as("cnt"))
        .orderBy( desc("cnt"))
        .select("*")



    df.printSchema()
    filterData.show(100)

  }

  def sparksess() = {
    val spark = SparkSession
      .builder()
      .master("local[3]")
      .appName("Airbnb")
      .getOrCreate()

    spark

  }



}

object airbnbobj extends airbnb {

  def main(args: Array[String]): Unit = {

    readCSV();

  }
}