package spark.training.example

import org.apache.log4j.{ Level, Logger }
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{ Row, SparkSession }
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

  private def initSystemProperties(args: Array[String]) = {
    val path = if(args.length >= 1) args(0) else "twitter.txt"

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
    //val warehouseLocation = "s3://ervin-shredinger/output-data/twits-d1"
    val warehouseLocation = "/Users/shredinger/Documents/DEVELOPMENT/dev-tools/spark/spark-warehouse"
    SparkSession
      .builder
      .config(conf)
      .config("spark.sql.warehouse.dir", warehouseLocation)
      .enableHiveSupport()
      .getOrCreate()
  }


  def twitterJob(args: Array[String]) = {
    initSystemProperties(args)

    val conf = new SparkConf().setAppName("TwitterData").setMaster("local[*]")
      .set("spark.network.timeout", "20s")
      .set("spark.executor.heartbeatInterval", "10s")


    val session = createSession(conf)
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

    val hdfsPath = "hdfs://sandbox-hdp.hortonworks.com/user/zeppelin/data/twits"
    val localPath = s"twits/twit_data"

    session.sql("DROP TABLE IF EXISTS twits")
    session.sql(
      """
        |CREATE TABLE twits ( latitude decimal, longitude decimal, country string, place string, text string )
        |STORED AS TEXTFILE
        |""".stripMargin
    )

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



      df.createOrReplaceTempView("t_twits")



      session.sql("INSERT INTO TABLE twits SELECT * FROM t_twits")
    }

    twits.start()
    ssc.start()


    ssc.awaitTermination()
    ssc.stop()
  }


  def experiment(args: Array[String]) = {
    import org.apache.spark.sql.types.{StringType, IntegerType, StructField, StructType }
    import org.apache.spark.sql.{ Row, SparkSession }
    import org.apache.spark.SparkConf
    import org.apache.spark.sql._

    def createDF(seq: =>Seq[Int])(spark: SparkSession) = {
      val schema =
        StructType(
          StructField("id", IntegerType, true) :: Nil
        )

      spark.createDataFrame(
        spark.sparkContext.parallelize(seq).map(Row(_)),
        schema
      )
    }


    val warehouseDir = "/Users/shredinger/Documents/DEVELOPMENT/dev-tools/spark/spark-warehouse/"
    val hdfsDataDir = "hdfs://sandbox-hdp.hortonworks.com/warehouse/temp/"

    val spark = SparkSession.builder().master("local").appName("Spark Hive Example").config("spark.sql.warehouse.dir", s"${hdfsDataDir}").config("spark.sql.parquet.cacheMetadata", false).enableHiveSupport().getOrCreate()

    import spark.implicits._
    import spark.sql

    val dataDir = "/Users/shredinger/Documents/DEVELOPMENT/dev-tools/spark/spark-warehouse/numbers"

//    spark
//      .sparkContext
//      .textFile("hdfs://sandbox-hdp.hortonworks.com/warehouse/temp/logs/hadoop-hdfs-datanode-sandbox-hdp.hortonworks.com.log.txt")
//      .count()
    
                                                

//    createDF(0 to 100)(spark)
//      .write
//      .mode(SaveMode.Append)
//      .format("hive")
//      .saveAsTable("ints")

    createDF(0 to 100)(spark).write.mode(SaveMode.Append).format("hive").saveAsTable(s"ints")
      
    


    //spark.catalog.refreshTable("ints")

    spark.stop()

  }

  def main(args: Array[String]): Unit = {

//    val warehouseLocation = "/Users/shredinger/Documents/DEVELOPMENT/dev-tools/spark/spark-warehouse/"
//
//    val spark = SparkSession
//      .builder()
//      .master("local")
//      .appName("Spark Hive Example")
//      .config("spark.sql.warehouse.dir", warehouseLocation)
//      .enableHiveSupport()
//      .getOrCreate()
//
//    import spark.implicits._
//    import spark.sql
//
//    sql("CREATE TABLE IF NOT EXISTS default.ids (id INT) USING hive")
//
//
//
//
//
//
//    spark.stop()



    experiment(args)


  }

}
