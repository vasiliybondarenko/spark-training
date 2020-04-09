package spark.training.example

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}
import org.apache.spark.sql.types.{
  BooleanType,
  DecimalType,
  DoubleType,
  IntegerType,
  LongType,
  StringType,
  StructField,
  StructType
}
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.twitter.TwitterUtils
import org.apache.spark.streaming.{Minutes, Seconds, StreamingContext}
import org.joda.time.{DateTime, LocalDate, LocalDateTime}
import twitter4j.{FilterQuery, Status}
import java.io.File
import java.util.{Calendar, Date}
import scala.io.Source

/**
  * Created by Bondarenko on Mar, 10, 2020
  *23:39.
  *Project: spark-training
  */
object TwitterApp {

  Logger.getLogger("org").setLevel(Level.ERROR)

  //todo:
  // CREATE TABLE IF NOT EXISTS twits ( latitude decimal, longitude decimal, country string, place string, text string ) LOCATION 'hdfs://sandbox-hdp.hortonworks.com/user/shredinger/output/test/twits'
  // CREATE EXTERNAL TABLE IF NOT EXISTS popular_twits ( country string, twit string, quotesCount bigint ) LOCATION 'hdfs://sandbox-hdp.hortonworks.com/user/shredinger/output/twits/popular_twits'
  // CREATE EXTERNAL TABLE IF NOT EXISTS top_twits ( country string, twit1 string, rating1 bigint, twit2 string, rating2 bigint, twit3 string, rating3 bigint ) LOCATION 'hdfs://sandbox-hdp.hortonworks.com/user/shredinger/output/twits/top_twits'

  private def initSystemProperties(args: Array[String]) = {
    val path = if (args.length >= 1) args(0) else "twitter.txt"

    Source
      .fromFile(path)
      .getLines()
      .map(_.split(" ").toList)
      .collect { case propName :: propValue :: Nil => s"twitter4j.oauth.$propName" -> propValue }
      .foreach { case (key, value) => System.setProperty(key, value) }
  }

  def createSession(conf: SparkConf, warehouseLocation: String) = {

    SparkSession.builder
      .config(conf)
      .config("spark.sql.warehouse.dir", warehouseLocation)
      .config("spark.sql.parquet.cacheMetadata", false)
      .enableHiveSupport()
      .getOrCreate()
  }

  def writeToHdfs(df: DataFrame, hdfsDataDir: String, tableName: String) = {
    df.write
      .mode(SaveMode.Overwrite)
      .option("path", s"${hdfsDataDir}/$tableName")
      .format("hive")
      .saveAsTable(tableName)
  }

  private def saveStream[K, V](spark: SparkSession, warehouseLocation: String, tableName: String)(
      stream: DStream[(K, V)]
  )(schema: StructType)(func: (K, V) => Row) = {
    stream.foreachRDD { (rdd, time) =>
      println(
        s"TIME: ${new DateTime(new Date(time.milliseconds)).toLocalTime.toString("hh:mm:ss")}"
      )
      val df = spark.createDataFrame(rdd.map { case (k, v) => func(k, v) }, schema)
      writeToHdfs(df, warehouseLocation, tableName)
    }
  }

  def twitterJob(args: Array[String]) = {
    import Math.max

    initSystemProperties(args)

    val warehouseLocation = "hdfs://sandbox-hdp.hortonworks.com/user/shredinger/output/twits"

    val conf = new SparkConf()
      .setAppName("TwitterData")
      .setMaster("local[*]")
      .set("spark.network.timeout", "20s")
      .set("spark.executor.heartbeatInterval", "10s")

    val session = createSession(conf, warehouseLocation)

    import session.implicits._

    val ssc = new StreamingContext(session.sparkContext, Seconds(20))
    ssc.checkpoint(s"$warehouseLocation/checkpoint")

    val locations = {
      val nycSouthWest = Array(-180.0, -90.0)
      val nycNorthEast = Array(180.0, 90.0)
      Array(nycSouthWest, nycNorthEast)
    }
    val query = new FilterQuery()
      .locations(locations: _*)
      .language("ru", "en")

    val twits: ReceiverInputDStream[Status] =
      TwitterUtils.createFilteredStream(ssc, None, Some(query))

    val twitsSchema =
      StructType(
        StructField("favourites", IntegerType, true) ::
          StructField("retweets", IntegerType, true) ::
          StructField("country", StringType, true) ::
          StructField("text", StringType, true) ::
          StructField("isRetweet", BooleanType, true) ::
          StructField("quotedStatusId", LongType, true) ::
          StructField("retweetedStatusId", LongType, true) ::
          StructField("retweetText", StringType, true) ::
          StructField("retweetCountry", StringType, true) :: Nil
      )

    type TWIT = (Long, (Int, Int, Option[String], String))

    val mostPopularTwitsSchema =
      StructType(
        StructField("country", StringType, true) ::
          StructField("twit", StringType, true) ::
          StructField("quotesCount", LongType, true) :: Nil
      )

    val topTwitsSchema =
      StructType(
        StructField("country", StringType, true) ::
          StructField("twit1", StringType, true) ::
          StructField("rating1", LongType, true) ::
          StructField("twit2", StringType, true) ::
          StructField("rating2", LongType, true) ::
          StructField("twit3", StringType, true) ::
          StructField("rating3", LongType, true) :: Nil
      )

    def maxCount(a: (String, Long), b: (String, Long)): (String, Long) = if (a._1 > b._1) a else b

    val windowDuration = Minutes(240)
    val slideDuration  = Seconds(60)

    val mostPopularTwits = twits
      .map(t =>
        Option(t.getPlace)
          .map(_.getCountry)
          .getOrElse("") -> Option(t.getQuotedStatus).map(_.getText).getOrElse("")
      )
      .filter { case (_, quotedTwit) => Option(quotedTwit).exists(_.nonEmpty) }
      .countByValueAndWindow(windowDuration, slideDuration)
      .map { case ((country, quotedTwit), count) => country -> (quotedTwit -> count) }
      .groupByKeyAndWindow(windowDuration, slideDuration)
      .mapValues { twitsForCountry =>
        twitsForCountry.toSeq
          .groupBy(_._1)
          .mapValues(_.map(_._2).sum)
          .toSeq
          .sortWith((a, b) => a._2 > b._2)
          .take(5)
          .filter {
            case (w, c) => Option(w).exists(_.nonEmpty) && Option(c).exists(_ > 0)
          }
      }

    saveStream(session, warehouseLocation, "top_twits")(mostPopularTwits)(topTwitsSchema) {
      case (country, topTwits) =>
        Row.fromSeq {
          val s =
            country +: (0 to 2).flatMap(i =>
              topTwits.map { case (w, c) => Seq(w, c) }.applyOrElse(i, (_: Int) => Seq("", -1L))
            )
          println(s)
          s
        }
    }

    twits.start()
    ssc.start()

    ssc.awaitTermination()
    ssc.stop()
  }

  def main(args: Array[String]): Unit = {

    twitterJob(args)

  }

}
