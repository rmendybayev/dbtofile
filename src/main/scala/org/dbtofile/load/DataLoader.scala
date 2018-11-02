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

import com.typesafe.config.Config
import org.apache.spark.sql._
import org.dbtofile.conf.TableInfo
import org.dbtofile.schema.{SchemaConverter, SchemaRegistry}
import org.dbtofile.util.Metrics


/**
  * Created by grinia on 8/16/16.
  */
object DataLoader {

  def loadData(dbInfo: TableInfo, sparkSession: SparkSession, appConf: Config, outputPath: String = "/tmp/dbtofile", outputFormat: String = "parquet"): Unit = {

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
      sparkSession.read.jdbc(dbInfo.url, dbInfo.sql, props)
    }

    val metrics = Metrics(dbInfo.table, dbInfo.outputPath)(sparkSession)
    val avroConverter = SchemaConverter.apply(appConf.getString("schema.registry.url"))

    val table = metrics.apply(readTable()).transform((tableDS: Dataset[Row]) => avroConverter.transformTable(tableDS, dbInfo.schema, dbInfo.table))
    val partitionNumber = Option(dbInfo.partition).getOrElse(1)

    table
      .repartition(partitionNumber)
      .write
      .mode(SaveMode.Append)
      .format(Option(dbInfo.outputFormat).getOrElse(outputFormat))
      .save(Option(dbInfo.outputPath).getOrElse(outputPath))
    metrics.reportStatistics()
  }
}
