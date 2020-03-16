package example


import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{ SaveMode, SparkSession }
import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.streaming._
import org.apache.spark.streaming.dstream.{ DStream, ReceiverInputDStream }
import org.apache.spark.streaming.twitter._
import twitter4j.{ FilterQuery, Status }
import scala.io.Source

/**
 * Created by Bondarenko on Mar, 10, 2020 
 23:39.
 Project: spark-training
 */
object TwitterApp extends App{

  private def initSystemProperties() = {
    Source.fromFile("twitter.txt").getLines()
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

  initSystemProperties()


  def createSession(conf: SparkConf) = {
    SparkSession
      .builder()
      .config(conf)
      //.enableHiveSupport()
      .getOrCreate()
  }



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
  val query = new FilterQuery().locations(locations : _*)
    .language("en")



  val twits: ReceiverInputDStream[Status] = TwitterUtils.createFilteredStream(ssc, None, Some(query))

  val hdfsPath = "hdfs://sandbox-hdp.hortonworks.com/user/zeppelin/data/twits1/twit_data"
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
    .saveAsTextFiles(hdfsPath, "")
  
  



  twits.start()
  ssc.start()

  //todo
  // create temp view like ratingsDs.createOrReplaceTempView("ratings")
  // HOW WE CAM QUERY SUCH TEMP VIEW IF ITS LIFETIME EQUALS TO SPARK SESSION LIFETIME??????!!!!!


  ssc.awaitTermination()
  ssc.stop()

}
