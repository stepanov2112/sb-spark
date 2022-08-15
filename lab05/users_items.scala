import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

import org.apache.spark.sql.functions.{col, udf}

import org.apache.spark.sql.functions.expr

import org.apache.spark.sql.functions._

import org.apache.spark.SparkContext

import org.apache.spark.SparkContext._

import org.apache.hadoop.fs.{FileSystem, Path}

import org.apache.spark.sql.SparkSession

import org.apache.spark.sql.functions.{col, from_unixtime, substring, udf}

import java.io._

 

import users_items.{input_dir, spark}

 

 

object users_items extends App {

 

 

  val spark = SparkSession.builder().appName("lab05").config("spark.master", "local[*]").getOrCreate()

  import spark.implicits._

 

  val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)

 

  spark.conf.set("spark.sql.session.timeZone", "UTC")

 

  val upd = spark.conf.get("spark.users_items.update")

  val input_dir = spark.conf.get("spark.users_items.input_dir")

  val output_dir = spark.conf.get("spark.users_items.output_dir")

 

  /*val upd = 0

  val output_dir = "output"

  val input_dir = "visits"

  */

 

 

  def exprc(myCols: Set[String], allCols: Set[String]) = {

    allCols.toList.map(x => x match {

      case x if myCols.contains(x) => col(x)

      case _ => lit(null).as(x)

    })

  }

 

 

  def getMaxDatefromHDFS(dir: String): String = {

 

    val status = fs.listStatus(new Path(output_dir))

    val z = status.filter(_.isDirectory).map(_.getPath.getName.toInt)

    if (z.isEmpty) {""} else {z.reduceLeft(_ max _).toString}

 

 

  }

 

  def getMaxDatefromLocalFS(dir: String): String = {

 

    val z = new File(dir)

      .listFiles

      .filter(_.isDirectory)

      .map(_.getName.toInt)

    if (z.isEmpty) {""} else {z.reduceLeft(_ max _).toString}

 

 

  }

 

 

 

  def getMaxDate(df: DataFrame): String = {

 

    df.select (max(from_unixtime(substring(col("timestamp"),1,10),"yyyyMMdd"))).collect()(0)(0).toString

 

  }

 

 

    val df_view = spark.read.json(input_dir.concat("/view"))

    val df_buy = spark.read.json(input_dir.concat("/buy"))

 

    val df_view_date = getMaxDate(df_view)

    val df_buy_date = getMaxDate(df_buy)

    val df_date = if (df_view_date >= df_buy_date) {df_view_date} else {df_buy_date}

 

 

 

 

    val df_view_cat = df_view

      .withColumn("lcat", lower(col("item_id")))

      .withColumn("lcat_", regexp_replace(col("lcat"),"-","_"))

      .withColumn("lcat__", regexp_replace(col("lcat_")," ","_"))

      .withColumn("lcat___", concat(lit("view_"),col("lcat__")))

      .select("uid","lcat___")

      .groupBy("uid")

      .pivot("lcat___")

      .count()

      .drop("lcat___")

 

    val df_buy_cat = df_buy

      .withColumn("lcat", lower(col("item_id")))

      .withColumn("lcat_", regexp_replace(col("lcat"),"-","_"))

      .withColumn("lcat__", regexp_replace(col("lcat_")," ","_"))

      .withColumn("lcat___", concat(lit("buy_"),col("lcat__")))

      .select("uid","lcat___")

      .groupBy("uid")

      .pivot("lcat___")

      .count()

      .drop("lcat___")

 

    val df_full = df_view_cat

      .join(df_buy_cat, Seq("uid"), "full")

      .na.fill(0).withColumn("da",lit(df_date))

 

  var z = ""

 

  if (upd == 1) {

      var z = if (output_dir.slice(0,4) == "file") {getMaxDatefromLocalFS(output_dir)} else {getMaxDatefromHDFS(output_dir)}

 

      if (z!="" && z != df_date) {

 

        val df_old = spark.read.parquet(output_dir.concat("/").concat(z)).withColumn("da",lit(z))

 

 

        val cols1 = df_full.columns.toSet

        val cols2 = df_old.columns.toSet

        val total = cols1 ++ cols2

 

 

        val df_all = df_full.select(exprc(cols1, total):_*)

                            .union(df_old.select(exprc(cols2, total):_*))

                            .na.fill(0)

                            .groupBy("uid")

                            .sum()

                            .drop("da")

 

        df_all.write.mode("Overwrite").format("parquet").save(output_dir.concat("/").concat(df_date))

 

      }

}

 

if (upd == 0 || z =="" || z==df_date) {

  val df_all = df_full.drop("da")

  df_all.write.mode("Overwrite").format("parquet").save(output_dir.concat("/").concat(df_date))

 

 

}

 

 

 

 

 

 

 

 

 

 

 

 

 

}