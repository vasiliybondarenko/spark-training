package example

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.ml._
import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.sql._
import org.apache.spark.sql.types.{DoubleType, IntegerType, StructField, StructType}

/**
  * Created by Bondarenko on Mar, 08, 2020
  * 23:04.
  * Project: spark-training
  */
object DataFrames {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    case class Rating(movieId: Int, rating: Double)

    val spark = SparkSession
      .builder()
      .master("local")
      .appName("test data frames")
      .getOrCreate()

    val ratings = spark.sparkContext
      .textFile(
        "/Users/shredinger/Documents/DEVELOPMENT/Projects/Scala/Spark_Hadoop/the-movies-dataset/ratings.csv"
      )
      .map(_.split(","))
      .filter(_(0).matches("[0-9]+"))
      .map(r => (r(1).toInt, r(2).toDouble))

    import spark.implicits._

    val ratingsDs = ratings.toDS()

    ratingsDs.printSchema()
    ratingsDs.createOrReplaceTempView("ratings")

    //ratingsDs.filter(ratingsDs("_2") > 3).select("_2").show()

    ratingsDs.filter(ratingsDs("_2") >= 4).groupBy("_1").count().orderBy($"count".desc).show()

    val highRatings = spark.sql("SELECT count(*) FROM ratings WHERE _2 >= 4")
    highRatings.collect().foreach(println)

  }
}
