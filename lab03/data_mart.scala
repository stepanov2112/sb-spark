%AddJar file:/data/home/stanislav.volkov/Lab_3/spark-cassandra-connector_2.11-2.4.3.jar
%AddJar file:/data/home/stanislav.volkov/Lab_3/elasticsearch-spark-20_2.11-8.3.1.jar
%AddJar file:/data/home/stanislav.volkov/Lab_3/postgresql-42.4.0.jar

import java.sql.DriverManager
import java.sql.Connection
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{StructType, StructField, StringType,IntegerType};
import java.sql.ResultSet

val url = "jdbc:postgresql://10.0.0.31:5432/stanislav_volkov?user=stanislav_volkov&password=owbW9cEp"
val query = s"GRANT SELECT on clients to PUBLIC"

val conn = DriverManager.getConnection(url)
val rs = conn.createStatement.execute(query)

import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.functions.{concat, lit}
import org.apache.spark.sql.functions.{col, udf}
import org.apache.spark.sql.functions._
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.sql.cassandra._
import com.datastax.spark.connector.cql.CassandraConnectorConf
import com.datastax.spark.connector.rdd.ReadConf

 val spark = SparkSession.builder().appName("lab03").config("spark.master", "yarn").getOrCreate()

def age_c  (age: Int) : String  = {


    val s : String =


      if(age <= 24){

        "18-24"

      } else if(age <= 34){

        "25-34"

      } else if(age <= 44){

        "35-44"

      } else if(age <= 54){

        "45-54"

      } else {

        ">=55"

      }

    return s }


val age_cat = udf { (v: Int) =>

    age_c(v)

  }

val elasticIndex = "visits"

val reader = spark.sqlContext.read
.format("org.elasticsearch.spark.sql")
.option("es.port","9200")
.option("es.nodes", "10.0.0.31")
.option("es.net.http.auth.user","stanislav_volkov")
.option("es.net.http.auth.pass","owbW9cEp")

val df_es = reader.load(elasticIndex)

val df_es_cat = df_es
.withColumn("lcat", lower(col("Category")))
.withColumn("lcat_", regexp_replace(col("lcat"), "-", "_"))
.withColumn("lcat__", regexp_replace(col("lcat_"), " ", "_"))
.withColumn("lcat___", concat(lit("shop_"), col("lcat__")))
.select("uid", "lcat___")
.groupBy("uid")
.pivot("lcat___")
.count()
.drop (col("lcat___"))

df_es_cat.show(10)

spark.conf.set("spark.cassandra.connection.host", "10.0.0.31")
spark.conf.set("spark.cassandra.connection.port", "9042")
spark.conf.set("spark.cassandra.output.consistency.level", "ANY")
spark.conf.set("spark.cassandra.input.consistency.level", "ONE")

val tableOpts = Map("table" -> "clients","keyspace" -> "labdata")

val df = spark
.read
.format("org.apache.spark.sql.cassandra")
.options(tableOpts)
.load()

df.show(2, 200, true)

val jdbcUrl = "jdbc:postgresql://10.0.0.31:5432/stanislav_volkov?user=stanislav_volkov&password=owbW9cEp"
val jdbcUrl1 = "jdbc:postgresql://10.0.0.31:5432/labdata?user=stanislav_volkov&password=owbW9cEp"

val df_pg = spark
.read
.format("jdbc")
.option("driver", "org.postgresql.Driver")
.option("url", jdbcUrl1)
.option("dbtable", "domain_cats")
.load()

df_pg.printSchema
df_pg.show(2, 200, true)

val parqDF = spark.read.parquet("/labs/laba03/weblogs.parquet")

val parqDF_ = parqDF.select(parqDF("*"), explode(parqDF("visits"))
.alias("visits_")).drop("visits")

val parqDF__ = parqDF_
.withColumn("url",col("visits_")
.getItem("url"))
.drop(col("visits_"))

val parqDF___ = parqDF__
.withColumn("domain_", callUDF("parse_url",col("url"), lit("HOST")))
.drop(col("url"))
.withColumn("domain", regexp_replace(col("domain_"),"www.",""))
.drop(col("domain_"))

parqDF___.show(10)

val web_cat = parqDF___.join(df_pg, Seq("domain"), "right").drop("domain")

val df_web_cat = web_cat
.withColumn("lcat", lower(col("category")))
.withColumn("lcat_", regexp_replace(col("lcat"),"-","_"))
.withColumn("lcat__", regexp_replace(col("lcat_")," ","_"))
.withColumn("lcat___", concat(lit("web_"),col("lcat__")))
.select("uid","lcat___")
.groupBy("uid")
.pivot("lcat___")
.count()
.drop("lcat___")


val df_cat = df.withColumn("age_cat",age_cat(col("age"))).drop("age")

df_cat.show(10)

val df_full = df_cat
    .join(df_es_cat, Seq("uid"), "left")
    .join(df_web_cat, Seq("uid"), "left")
    .na.fill(0)

df_full.show(10)

val typesMap = Map("string" -> "VARCHAR (100)", "int" -> "INTEGER","bigint" -> "INTEGER DEFAULT 0")

val primaryKey = "uid"

val ddlColumns = df_full.schema.fields.map { x =>

    if(x.name == primaryKey) {

      s"${x.name} ${typesMap(x.dataType.simpleString)} PRIMARY KEY"

    }

    else {

      s"${x.name} ${typesMap(x.dataType.simpleString)}"

    }

}.mkString(",")

val ddlQuery = s"CREATE TABLE IF NOT EXISTS clients ($ddlColumns);"

println(ddlQuery)

df_full.show(2, 200, true)

df_full
    .write
    .format("jdbc")
    .option("url", jdbcUrl)
    .option("driver", "org.postgresql.Driver")
    .option("dbtable", "clients")
    .mode("append")
    .save

val query = s"GRANT SELECT on clients to PUBLIC"
val rs = conn.createStatement.execute(query)

