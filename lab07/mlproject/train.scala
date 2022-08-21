import org.apache.spark.sql.{SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.ml.{Pipeline}
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.feature.{CountVectorizer, StringIndexer}

object train extends App{
    val spark = SparkSession.builder().appName("lab07_train")
      .config("spark.master", "yarn")
      .config("spark.sql.session.timeZone", "UTC")
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    import spark.implicits._

    val input_dir = "hdfs:///labs/laba07/laba07.json"
    val output_dir = "lab07_train/train"

    // Чтение данных обучения и сразу разворачивание доменов
    // + Обработка доменов
    val dfInput = spark.read.json(input_dir)
      .select('uid, 'gender_age, explode($"visits.url").as("src0"))
      .select('uid, 'gender_age, regexp_replace(regexp_replace($"src0", "%3A%2F%2F","://"), "3A",":").as("src1"))
      .select('uid, 'gender_age, regexp_replace($"src1", "((http)|(https))(://)","").as("src2"))
      .select('uid, 'gender_age, regexp_replace($"src2", "^www.","").as("url_"))
      .select('uid, 'gender_age, trim(split($"url_", "%2")(0)).as("url"))
      .select('uid, 'gender_age, split($"url", "/")(0).as("domain")
      )
    // Pipeline обучения
    val cv = new CountVectorizer()
      .setInputCol("domains")
      .setOutputCol("features")

    val indexer = new StringIndexer()
      .setInputCol("gender_age")
      .setOutputCol("label")

    val lr = new LogisticRegression()
      .setLabelCol("label")
      .setPredictionCol("prediction")
      .setMaxIter(10)
      .setRegParam(0.001)

    val pipeline = new Pipeline()
      .setStages(
          Array(cv, indexer, lr)
      )
    //DataSet для обучения
    val training = dfInput.groupBy("uid", "gender_age").agg(collect_list("domain").as("domains"))
    // Обучение
    val model = pipeline.fit(training)
    model.write.overwrite().save(output_dir)

    spark.stop()
}
