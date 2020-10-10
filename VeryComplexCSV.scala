package Sparksql

import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object VeryComplexCSV {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("complexCSV1").getOrCreate()
    //    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext

    import spark.implicits._
    import spark.sql

    val data="C:\\BigData\\DATASETS_NEW\\movies.csv"
    val df=spark.read.format("csv").option("header","true").option("inferschema","true").load(data)
    df.show(truncate = false)

    //val rs=df.withColumn("New_Title",expr("substring(N_Title,0,-5)"))
    //rs.show(false)

    val res=df.withColumn("N_Title",expr("rtrim(title)"))
      .withColumn("New_Title", expr("substring(N_Title, 1, length(N_Title)-6)"))
      .withColumn("Year",expr("substring(N_Title,-5,4)"))
      .withColumn("newgenres",split($"genres","\\|"))
      .select(col("*")+:(0 until 5).map(x=>col("newgenres")
      .getItem(x).as(s"col$x")):_*)

      //.get(col)
      .drop("title","N_Title")
      res.show(false)
    res.createOrReplaceTempView("TAB")
    //val res1=spark.sql("select movieId,New_Title,genres,Year from TAB ")
    //res1.show(false)

    spark.stop()
  }
}