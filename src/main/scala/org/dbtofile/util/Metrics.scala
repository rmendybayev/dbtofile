/*
 *     Licensed to the Apache Software Foundation (ASF) under one or more
 *     contributor license agreements.  See the NOTICE file distributed with
 *     this work for additional information regarding copyright ownership.
 *     The ASF licenses this file to You under the Apache License, Version 2.0
 *     (the "License"); you may not use this file except in compliance with
 *     the License.  You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *     Unless required by applicable law or agreed to in writing, software
 *     distributed under the License is distributed on an "AS IS" BASIS,
 *     WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *     See the License for the specific language governing permissions and
 *     limitations under the License.
 *
 */
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


