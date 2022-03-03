import org.apache.spark.sql.functions._

class concatfun extends config {

  def concatdata(): Unit = {

    val invoiceDF = spark_session.read
      .format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load("src/main/resources/invoices.csv")


    val concatData =

      invoiceDF.select(
              col("InvoiceNo"),
              col("StockCode"),
              col("Description"),
              col("Quantity"),
              col("InvoiceDate"),
              col("UnitPrice"),
              col("CustomerID"),
              col("Country"),
              concat(col("InvoiceNo"),lit("/"),col("StockCode")).as("concatdata")
            )


    invoiceDF.createOrReplaceTempView("tablename")
    spark_session.sqlContext.sql("select concat(UnitPrice,'',CustomerID) from tablename").show(5)


    concatData.show(10)


  }

}

object concatobject extends Serializable {

  def main(args: Array[String]): Unit = {
    val concatObj = new concatfun
    concatObj.concatdata();
  }
}
