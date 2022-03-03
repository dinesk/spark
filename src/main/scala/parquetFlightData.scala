


class parquetFlightData extends config {

  def parquetData = {

    spark_session.read
      .format("parquet")
      .option("path","src/main/resources/flightdata.parquet")
      .load()

  }

}
