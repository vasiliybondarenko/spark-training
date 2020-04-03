package spark.training.example

import example.Logging
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{ Seconds, StreamingContext }
import org.codehaus.jackson.map.ext.JodaDeserializers.LocalDateTimeDeserializer
import org.joda.time.LocalTime
import spark.training.example.TwitterApp.createSession
import scala.collection.mutable.Queue
import scala.concurrent.Future
import scala.io.Source
import scala.reflect.ClassTag


/**
 * Created by Bondarenko on Apr, 02, 2020 
 * 14:25.
 * Project: spark-training
 */
object SimpleStreamingExample extends App with Logging{

 lazy val session = {
  val conf = new SparkConf().setAppName("SimpleStreamingApp").setMaster("local[*]")
  SparkSession
    .builder
    .config(conf)
    .config("spark.sql.parquet.cacheMetadata", false)
    .enableHiveSupport()
    .getOrCreate()
 }

 def createStream[T: ClassTag](source: Seq[T], elementsInBatch: Int = 1) = {
  import scala.concurrent.ExecutionContext.Implicits.global


   val ssc = new StreamingContext(session.sparkContext, Seconds(1))
   val input = Queue(session.sparkContext.parallelize(List.empty[T]))

   ssc.checkpoint("hdfs://sandbox-hdp.hortonworks.com/user/shredinger/output/checkpoints")

   Future{
    source.sliding(elementsInBatch, elementsInBatch).foreach { col =>
     input.+=(session.sparkContext.parallelize(col))
    }
   }

   ssc -> ssc.queueStream(input)

 }

 def runStream[T: ClassTag](seq: Seq[T], elementsInBatch: Int = 1)(f: DStream[T] => Unit) = {
  val (ssc, numbers) = createStream(seq, elementsInBatch)
  f(numbers)

  ssc.start()
  numbers.start()

  ssc.awaitTermination()
  ssc.stop()
 }

def words = {
 Source.fromURL("https://spark.apache.org/docs/latest/streaming-programming-guide.html").getLines().flatMap { line =>
  line.split(Array(' ', ',', '.')).filter(w => w.matches("[a-zA-Z]+")).map(_.toLowerCase)
 }
}

def max(a: (String, Long), b: (String, Long)): (String, Long) = if(a._2 > b._2) a else b

val stopWords = Set("the", "a", "to", "of", "in", "is", "and", "you", "or", "on", "that", "this", "each", "be", "for", "by", "as", "if")

runStream(words.toSeq, 50) { words =>
  words.filter(!stopWords.contains(_))
    .countByValueAndWindow(Seconds(30), Seconds(10))
    .map { case (w, count) => Seq(w -> count) }
    .reduceByWindow(
     (a, b) =>
      (a ++  b)
        .groupBy(_._1)
        .map { case (w, counts) => w -> counts.map(_._2).sum }
        .toSeq.sortWith(_._2 > _._2)
        .take(10),
     Seconds(30),
     Seconds(10)).foreachRDD { rdd =>
      println("------------")
      rdd.foreach(_.foreach(println))
    }

}





}
