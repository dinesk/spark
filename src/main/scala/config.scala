import java.util.Properties

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.io.Source

trait config {

//  val spark_conf = new SparkConf()
//  spark_conf.set("spark.app.name","Learn")
//  spark_conf.set("spark.master","local[4]")

  val confFilePath = "src/main/resources/spark.conf"

  val spark_conf = {
    val spark_conf = new SparkConf()
    val pros = new Properties()
    pros.load(Source.fromFile(confFilePath).bufferedReader())
    pros.forEach((k,v) => spark_conf.set(k.toString,v.toString) )

    spark_conf
  }


  val spark_session = {

    val spark = SparkSession
        .builder()
        .config(spark_conf)
        .enableHiveSupport()
        .getOrCreate()

    spark

  }

  def spark_session_stop() = {
    spark_session.stop()
  }

}