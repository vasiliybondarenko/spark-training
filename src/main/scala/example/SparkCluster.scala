package example

package wiki.spark

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._
import org.apache.spark.sql.SparkSession
import scala.util.{Success, Try}

/**
  * Created by Bondarenko on Mar, 06, 2020
 12:32.
 Project: Wikipedia
  */
object SparkCluster extends App {

  Logger.getLogger("org").setLevel(Level.ERROR)

  val master = "spark://192.168.160.245:7077"
  val master1 = "spark://vbondarenko-mac.local:7077"

  val spark = SparkSession
    .builder()
    .master("local")
    .appName("test")
    .getOrCreate()

  //spark.sparkContext.broadcast()

  def moviesRDD = {
    val lines = spark.sparkContext.textFile(
      "/Users/shredinger/Documents/DEVELOPMENT/Projects/Scala/Spark_Hadoop/the-movies-dataset/movies_metadata.csv"
    )
    val strPattern = """"([^"]*)"""".r

    lines
      .map { x =>
        Try(
          strPattern.replaceAllIn(x, m => s"${m.group(1).replaceAll(",", "'")}")
        )
      }
      .collect { case Success(value) => value }
      .map { x =>
        x.split(",")
      }
      .filter(_.size > 14)
      .map { fields =>
        fields(5) -> (fields(8) -> fields(14))
      }
      .filter(x => x._1.matches("[0-9]+"))
      .map { case (mId, (title, date)) => mId.toInt -> (title -> date) }
  }

  def ratingsRDD =
    spark.sparkContext
      .textFile(
        "/Users/shredinger/Documents/DEVELOPMENT/Projects/Scala/Spark_Hadoop/the-movies-dataset/ratings.csv"
      )
      .map(_.split(","))
      .filter(_(0).matches("[0-9]+"))
      .map(r => r(1).toInt -> r(2).toDouble)

  ratingsRDD
    .filter(_._2 >= 4)
    .mapValues(_ => 1)
    .reduceByKey(_ + _)
    .sortBy(_._2, false)
    .join(moviesRDD)
    .take(100)
    .foreach(println)

  spark.stop()

}
