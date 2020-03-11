package example


import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.streaming._
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.streaming.twitter._
import twitter4j.Status
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

  initSystemProperties()

  val appName = "TwitterData"
  val conf = new SparkConf().setAppName(appName).setMaster("local[*]")
    .set("spark.network.timeout", "20s")
    .set("spark.executor.heartbeatInterval", "10s")

  val ssc = new StreamingContext(conf, Seconds(5))

  val twits: ReceiverInputDStream[Status] = TwitterUtils.createStream(ssc, None)


  twits.map(_.getText).saveAsTextFiles("twits-", "twits.txt")

  twits.start()
  ssc.start()

  ssc.awaitTermination()
  ssc.stop()

}
