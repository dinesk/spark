import org.apache.spark.sql.functions.{round, _}

class FlightDataAgg extends config {

  def calAgg() = {

    val invoiceDF = spark_session.read
      .format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load("src/main/resources/invoices.csv")

    invoiceDF.select(
      count("*").as("Count *"),
      sum("Quantity").as("TotalQuantity"),
      avg("UnitPrice").as("AvgPrice"),
      countDistinct("InvoiceNo").as("CountDistinct")
    )

    invoiceDF.selectExpr(
      "count(1) as `count 1`",
      "count(StockCode) as `count field`",
      "sum(Quantity) as TotalQuantity",
      "avg(UnitPrice) as AvgPrice"
    ).show()

    invoiceDF.createOrReplaceTempView("sales")
    val summarySQL = spark_session.sql(
      """
        |SELECT Country, InvoiceNo,
        | sum(Quantity) as TotalQuantity,
        | round(sum(Quantity*UnitPrice),2) as InvoiceValue
        | FROM sales
        | GROUP BY Country, InvoiceNo
        |""".stripMargin)

    //summarySQL.show()

    val totalQuanty = sum("Quantity").as("TotalQuantity")
    val invoiceValue = round(sum(expr("Quantity*UnitPrice")), 2).as("InvoiceValue")
    val invoiceValueExpr = expr("round(sum(Quantity*UnitPrice),2) as InvoiceValueExpr")

    val summaryDF = invoiceDF
                    .groupBy("Country", "InvoiceNo")
                    .agg(totalQuanty,invoiceValue, invoiceValueExpr)
                    .orderBy("TotalQuantity","InvoiceValue")

    summaryDF.show()
  }
}

object FlightDataAggObj extends Serializable {
  def main(args: Array[String]): Unit = {

    val fdg = new FlightDataAgg
    fdg.calAgg()
  }
}