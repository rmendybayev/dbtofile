/*
 *  Licensed to the Apache Software Foundation (ASF) under one or more
 *  contributor license agreements.  See the NOTICE file distributed with
 *  this work for additional information regarding copyright ownership.
 *  The ASF licenses this file to You under the Apache License, Version 2.0
 *  (the "License"); you may not use this file except in compliance with
 *  the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.dbtofile.load

import java.util.Properties

import org.apache.spark.sql.{SQLContext, SaveMode, SparkSession}
import org.dbtofile.conf.TableInfo
import org.dbtofile.schema.{SchemaConverter, SchemaRegistry}
import org.dbtofile.util.Metrics


/**
  * Created by grinia on 8/16/16.
  */
object DataLoader {

  def loadData(dbInfo: TableInfo, sparkSession: SparkSession, outputPath: String = null, outputFormat: String = null): Unit = {

    if (!dbInfo.load) {
      return
    }

    def readTable() = {
      val props = new Properties()
      props.setProperty("driver", "oracle.jdbc.driver.OracleDriver")
      props.setProperty("password", dbInfo.password)
      props.setProperty("user", dbInfo.user)
      props.setProperty("url", dbInfo.url)
      //      props.setProperty("partitionColumn", dbInfo.partCol) FIXME: add config for parallelization
      //      props.setProperty("lowerBound", dbInfo.lowerB)
      //      props.setProperty("upperBound", dbInfo.upperB)
      //      props.setProperty("numPartitions", partitionNumber.toString)
      sparkSession.read.jdbc(dbInfo.url, dbInfo.table, props)
    }

    val metrics = Metrics(dbInfo.table, dbInfo.outputPath)(sparkSession)
    val avroConverter = SchemaConverter.apply("") //FIXME: add config

    val table = metrics.apply(readTable()).transform(avroConverter.transformTable(_, ""))

    table
      .repartition(1)
      .write
      .mode(SaveMode.Append)
      .format(Option(outputFormat).getOrElse(dbInfo.outputFormat))
      .save(Option(outputFormat).getOrElse(dbInfo.outputPath))
    metrics.reportStatistics()
  }
}
