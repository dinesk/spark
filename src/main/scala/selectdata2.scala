import java.util.Properties

import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.io.Source

object selectdata2 extends Serializable {
  @transient lazy val logger: Logger = Logger.getLogger(getClass.getName)

  def main(args: Array[String]): Unit = {

    if (args.length == 0) {
      logger.error("Usage: HelloSpark filename")
      System.exit(1)
    }

    logger.info("Starting Hello Spark")
    val spark = SparkSession.builder()
      .config(getSparkAppConf)
      .getOrCreate()
    //logger.info("spark.conf=" + spark.conf.getAll.toString())

    val surveyRawDF = loadSurveyDF(spark, "src/main/resources/sample.csv")
    val partitionedSurveyDF = surveyRawDF.repartition(2)
    val countDF = countByCountry(partitionedSurveyDF)
    countDF.foreach(row => {
      logger.info("Country: " + row.getString(0) + " Count: " + row.getLong(1))
    })

    logger.info(countDF.collect().mkString("->"))
    countDF.show()

    logger.info("Finished Hello Spark")
    //scala.io.StdIn.readLine()
    spark.stop()
  }

  def countByCountry(surveyDF: DataFrame): DataFrame = {
    surveyDF.where("Age < 40")
      .select("Age", "Gender", "Country", "state")
      .groupBy("Country")
      .count()
  }

  def loadSurveyDF(spark: SparkSession, dataFile: String): DataFrame = {
    spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(dataFile)
  }

  def getSparkAppConf: SparkConf = {
    val sparkAppConf = new SparkConf

    val props = new Properties
    props.load(Source.fromFile("src/main/resources/spark.conf").bufferedReader())
    props.forEach((k, v) => sparkAppConf.set(k.toString, v.toString))

    sparkAppConf
  }

}