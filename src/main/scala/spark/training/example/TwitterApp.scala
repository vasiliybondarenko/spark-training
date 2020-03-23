package spark.training.example

import org.apache.log4j.{ Level, Logger }
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.dstream.{ DStream, ReceiverInputDStream }
import org.apache.spark.streaming.twitter.TwitterUtils
import org.apache.spark.streaming.{ Seconds, StreamingContext }
import twitter4j.{ FilterQuery, Status }
import scala.io.Source

/**
 * Created by Bondarenko on Mar, 10, 2020
 *23:39.
 *Project: spark-training
 */
object TwitterApp {

  Logger.getLogger("org").setLevel(Level.ERROR)

  private def initSystemProperties(args: Array[String]) = {
    val path = if(args.length >= 2) args(1) else "twitter.txt"

    Source.fromFile(path).getLines()
      .map(_.split(" ").toList)
      .collect { case propName :: propValue :: Nil => s"twitter4j.oauth.$propName" -> propValue }
      .foreach { case (key, value) => System.setProperty(key, value) }
  }

  private def writeToHive(session: SparkSession)(stream: DStream[(Long, String)]) = {
    stream.saveAsTextFiles("", "")

    stream.foreachRDD { rdd =>
      println(s"SAVING RDD ...")
      saveRDD(session, rdd)
    }
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




  def createSession(conf: SparkConf) = {
    SparkSession
      .builder()
      .config(conf)
      //.enableHiveSupport()
      .getOrCreate()
  }





  def main(args: Array[String]) = {
    initSystemProperties(args)

    val outputPath = if(args.length == 0) "twits-data" else args(0)

    val conf = new SparkConf().setAppName("TwitterData").setMaster("local[*]")
      .set("spark.network.timeout", "20s")
      .set("spark.executor.heartbeatInterval", "10s")


    val session = createSession(conf)


    val ssc = new StreamingContext(session.sparkContext, Seconds(5))


    val locations = {
      val nycSouthWest = Array(-10.0, 53.0)
      val nycNorthEast = Array(30.0, 54.0)
      Array(nycSouthWest, nycNorthEast)
    }
    val query = new FilterQuery().locations(locations: _*)
      .language("en")


    val twits: ReceiverInputDStream[Status] = TwitterUtils.createFilteredStream(ssc, None, Some(query))

    val hdfsPath = "hdfs://sandbox-hdp.hortonworks.com/user/zeppelin/data/twits"
    val localPath = s"twits/twit_data"

    twits
      .map(t =>
        t.getId -> (
          Option(t.getGeoLocation).map(_.getLatitude),
          Option(t.getGeoLocation).map(_.getLongitude),
          Option(t.getPlace).map(_.getCountry),
          Option(t.getPlace).map(_.getFullName),
          t.getText
        )
      )
      .saveAsTextFiles(outputPath, "")


    twits.start()
    ssc.start()

    //todo
    // create temp view like ratingsDs.createOrReplaceTempView("ratings")
    // HOW WE CAM QUERY SUCH TEMP VIEW IF ITS LIFETIME EQUALS TO SPARK SESSION LIFETIME??????!!!!!


    ssc.awaitTermination()
    ssc.stop()
  }

}
