import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window


class window_func  extends config {

  def fetchdata(): Unit ={

    import spark_session.implicits._

    val columns = Seq("id","date","amt")

    val data = List(
      ("1","2016-03-01 00:00:00.0","25.7262"),
      ("1","2016-03-02 00:00:00.0","26.6861"),
      ("1","2016-03-03 00:00:00.0","27.0688"),
      ("1","2016-03-04 00:00:00.0","28.8077"),
      ("1","2016-03-07 00:00:00.0","29.6904"),
      ("1","2016-03-08 00:00:00.0","26.9298"),
      ("1","2016-03-09 00:00:00.0","27.2492"),
      ("1","2016-03-10 00:00:00.0","26.278|")
    )

    val df = spark_session.createDataFrame(data).toDF(columns:_*)

    df.show()

    val byYearOrderByAmt = Window.partitionBy(year($"date")).orderBy("amt")

    df.withColumn("rank", dense_rank() over byYearOrderByAmt).show

  }
}


object window_func_obj extends Serializable {
  def main(args: Array[String]): Unit = {

      val objdata = new window_func()
      objdata.fetchdata()

  }
}