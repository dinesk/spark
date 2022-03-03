import org.apache.log4j.Logger
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DateType, IntegerType, StringType, StructField, StructType, DataType}


class flightData extends config {

  @transient lazy val logger: Logger =Logger.getLogger(getClass.getName)

  def fetchData( v:String) = {

    logger.info("fetch method called ")


    val flightSchemaStruct = StructType(List(
      StructField("FL_DATE", DateType),
      StructField("OP_CARRIER", StringType),
      StructField("OP_CARRIER_FL_NUM", IntegerType),
      StructField("ORIGIN", StringType),
      StructField("ORIGIN_CITY_NAME", StringType),
      StructField("DEST", StringType),
      StructField("DEST_CITY_NAME", StringType),
      StructField("CRS_DEP_TIME", IntegerType),
      StructField("DEP_TIME", IntegerType),
      StructField("WHEELS_ON", IntegerType),
      StructField("TAXI_IN", IntegerType),
      StructField("CRS_ARR_TIME", IntegerType),
      StructField("ARR_TIME", IntegerType),
      StructField("CANCELLED", IntegerType),
      StructField("DISTANCE", IntegerType)
    ))



    val flightSchemaDDL = "FL_DATE, OP_CARRIER, OP_CARRIER_FL_NUM, ORIGIN, ORIGIN_CITY_NAME, DEST,DEST_CITY_NAME " +
      "CRS_DEP_TIME, DEP_TIME, WHEELS_ON, TAXI_IN , CRS_ARR_TIME, ARR_TIME, CANCELLED, DISTANCE"

    val filePath = "src/main/resources/flight-time.csv"


    val csvdataDF = spark_session
      .read
      .option("header",true)
      .option("format","csv")
      .option("infersSchema",true)
      .option("path",filePath)
      .option("dateFormat","y-m-d")
      .schema(flightSchemaStruct)
      .csv()

    //    csvdataDF.printSchema()
    csvdataDF.show()

    val finalDF = csvdataDF
        .repartition(3)
      .write.mode("overwrite")
      .format("json")
      //.format("parquet")
      //.option("compression", "snappy")
      .mode(SaveMode.Overwrite)
      .option("maxRecordsPerFile",10000)
      .partitionBy("OP_CARRIER","ORIGIN")
      .parquet("src/main/store_data/flightparquetdata.parquet")



    //scala.io.SdIn.readLine()

      //.groupBy().count().show()


    spark_session_stop()

    println("main data")
  }



}

object flightDataObj extends Serializable{
  def main(args: Array[String]): Unit = {

    val obj = new flightData;
    obj.fetchData("dsf");

  }
}
