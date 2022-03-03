import org.apache.log4j.Logger
import org.apache.spark.sql.functions._

class selectdata extends config {

  @transient lazy val logger: Logger = Logger.getLogger(getClass.getName)

  def fetchData(v: String) = {

    logger.info("fetch method called ")
    val filePath = "src/main/resources/sample.csv"

    val noPartition = 2

    val csvdataDF = spark_session
      .read
      .option("header", true)
      .option("infersSchema", true)
      .csv(filePath)

    csvdataDF.show()

    val csvdataDF2 = csvdataDF.repartition(noPartition)

    val filterDataDF = csvdataDF2.where("Age < 40")
    val selectDataDF = filterDataDF.select(col("Age"), col("Gender"), col("Country"), col("state"))
    val countDF = selectDataDF.groupBy("country").count()

    countDF.show()

    logger.info(countDF.collect().mkString("->"))

    scala.io.StdIn.readLine()

    println("main data")
  }
}


object selectdataObj extends Serializable {
  def main(args: Array[String]): Unit = {

    val obj = new selectdata;
    obj.fetchData("dsf");

  }
}
