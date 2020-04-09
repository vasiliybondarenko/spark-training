package spark.training.example

/**
  * Created by Bondarenko on Mar, 27, 2020
 21:40.
 Project: spark-training
  */
trait Utils {
  implicit class Printer[T](o: T) {
    def show(f: T => String) = println(f(o))
  }
}
