package spark.training.example

import example.Logging
import org.apache.spark.SparkConf
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}

/**
  * Created by Bondarenko on Mar, 19, 2020
  *11:12.
  *Project: spark-training
  */
object SimpleSparkJob extends Logging with Utils {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .master("local")
      .config(new SparkConf())
      .appName("test data frames")
      .getOrCreate()

    spark.sparkContext
      .textFile("hdfs://sandbox-hdp.hortonworks.com/user/shredinger/datanode.log.txt")
      .count()
      .show(c => s"COUNT: $c")
    //before run:
    // sudo -u hdfs hdfs dfs -mkdir /user/shredinger
    // sudo -u hdfs hdfs dfs -chown shredinger /user/shredinger
    // docker cp ~/Downloads/datanode.log.txt sandbox-hdp:/tmp
    // sudo -u hdfs hdfs dfs -copyFromLocal /tmp/datanode.log.txt hdfs://sandbox-hdp.hortonworks.com/user/shredinger/datanode.log.txt
    // HADOOP configs:
    // ls -alGh /etc/hadoop/3.0.1.0-187/0/

    //in zeppeling:
    // CREATE EXTERNAL TABLE IF NOT EXISTS ints(id int) LOCATION 'hdfs://sandbox-hdp.hortonworks.com/user/shredinger/output/test/ints'

  }
}
