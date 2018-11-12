package org.dbtofile.util

import org.apache.log4j.Logger
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.apache.spark.util.LongAccumulator
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods.{compact, render}

class Metrics(tableName: String, outputPath: String, accum: LongAccumulator) extends Serializable {
  @transient private lazy val log = Logger.getLogger("org.dbtofile")

  def apply(df: Dataset[Row]): Dataset[Row] = {
    df.filter { a => inc(); true }
  }

  def inc(): Unit = {
    accum.add(1L)
  }

  def reportStatistics(): Unit = {
    val stat = ("outputPath" -> outputPath) ~ ("processed" -> accum.sum) ~ ("tableName" -> tableName)
    log.info(s"[STATISTICS]:${compact(render(stat))}")
  }
}


object Metrics {
  def apply(tableName: String, outputPath: String)(implicit spark:SparkSession): Metrics = {
    val accumulator: LongAccumulator = spark.sparkContext.longAccumulator(s"$tableName-counter")
    new Metrics(tableName, outputPath, accumulator)
  }
}


