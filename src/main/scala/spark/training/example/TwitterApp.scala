package spark.training.example

import org.apache.log4j.{ Level, Logger }
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{ DataFrame, Row, SaveMode, SparkSession }
import org.apache.spark.sql.types.{ DecimalType, DoubleType, IntegerType, StringType, StructField, StructType }
import org.apache.spark.streaming.dstream.{ DStream, ReceiverInputDStream }
import org.apache.spark.streaming.twitter.TwitterUtils
import org.apache.spark.streaming.{ Seconds, StreamingContext }
import twitter4j.{ FilterQuery, Status }
import java.io.File
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
      .mode(SaveMode.Append)
      .option("path", s"${hdfsDataDir}/$tableName")
      .format("hive")
      .saveAsTable(tableName)
  }


  def twitterJob(args: Array[String]) = {
    initSystemProperties(args)

    val warehouseLocation = "hdfs://sandbox-hdp.hortonworks.com/user/shredinger/output/twits"

    val conf = new SparkConf().setAppName("TwitterData").setMaster("local[*]")
      .set("spark.network.timeout", "20s")
      .set("spark.executor.heartbeatInterval", "10s")


    val session = createSession(conf, warehouseLocation)
    import session.implicits._


    val ssc = new StreamingContext(session.sparkContext, Seconds(5))


    val locations = {
      val nycSouthWest = Array(-10.0, 53.0)
      val nycNorthEast = Array(30.0, 54.0)
      Array(nycSouthWest, nycNorthEast)
    }
    val query = new FilterQuery().locations(locations: _*)
      .language("en")


    val twits: ReceiverInputDStream[Status] = TwitterUtils.createFilteredStream(ssc, None, Some(query))

    val twitsSchema =
      StructType(
        StructField("latitude", DoubleType, true) ::
          StructField("longitude", DoubleType, true) ::
          StructField("country", StringType, true) ::
          StructField("place", StringType, true) ::
          StructField("text", StringType, true) :: Nil
      )


    twits
      .map(t =>
        t.getId -> (
          Option(t.getGeoLocation).map(_.getLatitude),
          Option(t.getGeoLocation).map(_.getLongitude),
          Option(t.getPlace).map(_.getCountry),
          Option(t.getPlace).map(_.getFullName),
          t.getText
        )
      ).foreachRDD { (rdd, time) =>

      val df = session.createDataFrame(
        rdd.map { case (_, (latitude, longitude, country, place, text)) => Row(
          latitude.getOrElse(0.0),
          longitude.getOrElse(0.0),
          country.getOrElse(""),
          place.getOrElse(""),
          text)
        },
        twitsSchema
      )


      writeToHdfs(df, warehouseLocation, "twits0")


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
