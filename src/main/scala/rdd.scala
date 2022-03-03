class rdd extends config {

  def mapflatmap: Unit ={

    val maprdd = spark_session.sparkContext.parallelize(Seq("Roses are red", "Violets are blue"))  // lines

    val x = spark_session.sparkContext.parallelize(List("spark", "map", "example",  "sample", "example"), 10)
    val y = x.map(x => (x, 1))


    println("*"*100)
    println(y.collect().foreach(println))
    println("*"*100)
  }
}

object rddObject extends Serializable {

  def main(args: Array[String]): Unit = {

    val rdd = new rdd()
    rdd.mapflatmap
  }
}
