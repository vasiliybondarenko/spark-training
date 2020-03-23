package example

import org.apache.log4j.{ Level, Logger }

/**
 * Created by Bondarenko on Mar, 19, 2020 
 * 11:13.
 * Project: spark-training
 */
trait Logging {
  Logger.getLogger("org").setLevel(Level.ERROR)

  lazy val logger = Logger.getLogger(this.getClass)
}
