sbt 'assembly'
spark-submit --class example.wiki.spark.SparkCluster --master yarn  --deploy-mode client  target/scala-2.11/spark-training-assembly-0.1.0-SNAPSHOT.jar 'arg'
