package spark.training.example

import org.apache.log4j.{ Level, Logger }
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{ DataFrame, Row, SaveMode, SparkSession }
import org.apache.spark.sql.types.{ BooleanType, DecimalType, DoubleType, IntegerType, LongType, StringType, StructField, StructType }
import org.apache.spark.streaming.dstream.{ DStream, ReceiverInputDStream }
import org.apache.spark.streaming.twitter.TwitterUtils
import org.apache.spark.streaming.{ Minutes, Seconds, StreamingContext }
import org.joda.time.{ DateTime, LocalDate, LocalDateTime }
import twitter4j.{ FilterQuery, Status }
import java.io.File
import java.util.{ Calendar, Date }
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


  private def initSystemProperties(args: Array[String]) = {
    val path = if(args.length >= 1) args(0) else "twitter.txt"

    Source.fromFile(path).getLines()
      .map(_.split(" ").toList)
      .collect { case propName :: propValue :: Nil => s"twitter4j.oauth.$propName" -> propValue }
      .foreach { case (key, value) => System.setProperty(key, value) }
  }


  private def saveRDD(session: SparkSession, rdd: RDD[(Long, String)]) = {
    import org.apache.spark.sql._
    import org.apache.spark.sql.types._


    val schema =
      StructType(
        StructField("id", LongType, false) ::
          StructField("text", StringType, true) :: Nil)


    val sqlContext = session.sqlContext
    val df = sqlContext.createDataFrame(rdd.map(v => Row(v._1, v._2)), schema)

    df
      .write
      .format("hive")
      .mode(SaveMode.Append)
      .saveAsTable("default.twits")
  }




  def createSession(conf: SparkConf, warehouseLocation: String) = {

    SparkSession
      .builder
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


  private def saveStream[K, V](spark: SparkSession, warehouseLocation: String, tableName: String)(stream: DStream[(K, V)])(schema: StructType)(func: (K, V) => Row) = {
    stream.foreachRDD { (rdd, time) =>
      println(s"TIME: ${new DateTime(new Date(time.milliseconds)).toLocalTime.toString("hh:mm:ss")}")
      val df = spark.createDataFrame(rdd.map { case (k, v) => func(k, v) }, schema)
      writeToHdfs(df, warehouseLocation, tableName)
    }
  }

  def twitterJob(args: Array[String]) = {
    import Math.max

    initSystemProperties(args)


    val warehouseLocation = "hdfs://sandbox-hdp.hortonworks.com/user/shredinger/output/twits"

    val conf = new SparkConf().setAppName("TwitterData").setMaster("local[*]")
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
    val query = new FilterQuery().locations(locations: _*)
      .language("ru", "en")



    val twits: ReceiverInputDStream[Status] = TwitterUtils.createFilteredStream(ssc, None, Some(query))


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

    type TWIT =  (Long, (Int, Int, Option[String], String))

    val shortTwits = twits.filter { t =>
      Option(t.getQuotedStatus).isDefined
    }
      .map(t =>
        t.getId -> (
          t.getFavoriteCount,
          t.getRetweetCount,
          Option(t.getPlace).map(_.getCountry),
          t.getText,
          t.isRetweet,
          t.getQuotedStatusId,
          Option(t.getRetweetedStatus).map(_.getId).getOrElse(-1L),
          Option(t.getQuotedStatus).map(_.getText).getOrElse(""),
          Option(t.getRetweetedStatus).flatMap(t => Option(t.getPlace)).map(_.getCountry).getOrElse("")
        )
      )

    val mostPopularTwitsSchema =
      StructType(
        StructField("country", StringType, true) ::
        StructField("twit", StringType, true) ::
        StructField("quotesCount", LongType, true) :: Nil
      )

    def maxCount(a: (String, Long), b: (String, Long)): (String, Long) = if(a._1 > b._1) a else b

    val windowDuration = Minutes(60)
    val slideDuration = Minutes(10)

    val mostPopularTwits = twits
      .map(t =>
        Option(t.getPlace).map(_.getCountry).getOrElse("") ->  Option(t.getQuotedStatus).map(_.getText).getOrElse("")
      ).countByValueAndWindow(windowDuration, windowDuration)
      .map { case ((country, quotedTwit), count) => country -> (quotedTwit, count) }
      .reduceByKeyAndWindow(maxCount(_, _), windowDuration, slideDuration)

    saveStream(session, warehouseLocation, "popular_twits1")(mostPopularTwits)(mostPopularTwitsSchema) {
      case (country, (twit, count)) => Row(country, twit, count)
    }

//    saveStream(session, warehouseLocation, "retweets")(shortTwits)(twitsSchema) {
//      case (_, (favourites, retweets, country, text, isRetweet, quotedId, retweetedId, retweetText, retweetCountry)) =>
//        Row(favourites, retweets, country.getOrElse("N/A"), text, isRetweet, quotedId, retweetedId, retweetText, retweetCountry)
//    }

    twits.start()
    ssc.start()


    ssc.awaitTermination()
    ssc.stop()
  }

  def main(args: Array[String]): Unit = {

     twitterJob(args)

  }

}
