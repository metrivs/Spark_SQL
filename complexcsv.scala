package Sparksql

import org.apache.spark.sql._
import org.apache.spark.sql.functions._
//val data="C:\\BigData\\DATASETS_NEW\\10000Records.csv"
object complexcsv {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("veryComplexcsv").getOrCreate()
    //    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext

    import spark.implicits._
    import spark.sql
    val data = "C:\\BigData\\DATASETS_NEW\\movies.csv"
    val df = spark.read.format("csv").option("header","true").option("inferSchema","true").load(data)
    df.show(false)
     val res = df.withColumn("moviename",expr("substring(title,1,length(title)-6)"))
      .withColumn("newYear",expr("substring(title,-5,4)"))
      .withColumn("newgenres",split($"genres","\\|"))
            .select(col("*")+:(0 until 5).map(x =>col("newgenres")
            .getItem(x).as(s"col$x")): _*)
      .drop("title","genres","moviename","newgenres")


    res.show(false)
    //find Comedy movies
    res.createOrReplaceTempView("tab")
    //val result = spark.sql("select * from tab where  'Romance' in (col0, col1,col2,col3,col4)")
    val result = spark.sql("select * from tab")
    result.show()

    spark.stop()
  }
}