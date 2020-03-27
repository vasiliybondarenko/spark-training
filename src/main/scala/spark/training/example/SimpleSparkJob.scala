package spark.training.example

import example.Logging
import org.apache.spark.sql.types.{ IntegerType, StringType, StructField, StructType }
import org.apache.spark.sql.{ Row, SparkSession }

/**
 * Created by Bondarenko on Mar, 19, 2020
 *11:12.
 *Project: spark-training
 */
object SimpleSparkJob extends Logging{
 def main(args: Array[String]): Unit = {
  val spark = SparkSession
    .builder()
    .appName("test data frames")
    .getOrCreate()


  val schema =
       StructType(
             StructField("id", IntegerType, true) :: Nil
       )

   val messagesSchema =
     StructType(
       StructField("text", StringType, true) :: Nil
     )
   val messagesRdd = spark.sparkContext.parallelize(Range(0, 10000)).map(_.toString)
   //val df = spark.createDataFrame(messagesRdd.map(Row(_)), messagesSchema)


   val rdd = spark.sparkContext.parallelize(Range(0, 10000))



   val df = spark.createDataFrame(rdd.map(Row(_)), schema)
   df.foreach(logger.warn(_))

   //df.write.csv("s3://ervin-shredinger/output-data")



 }
}
