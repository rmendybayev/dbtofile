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

package org.dbtofile.conf

import java.io.{File, FileInputStream, FileWriter}
import java.sql.Date
import java.time.LocalDate

import org.yaml.snakeyaml.Yaml
import org.yaml.snakeyaml.constructor.Constructor

import scala.beans.BeanProperty
import scala.collection.mutable.ListBuffer
import scala.concurrent.duration.Duration


/**
  * Created by grinia on 8/16/16.
  */


case class TableList(
  @BeanProperty var tables: Array[TableInfo] = Array.empty[TableInfo],
  @BeanProperty var merges: Array[MergeInfo] = Array.empty[MergeInfo]) {
  def this() = {
    this(Array.empty[TableInfo], null)
  }
}

case class TableInfo (
     @BeanProperty var url: String,
     @BeanProperty var table: String,
     @BeanProperty var schema: String,
     @BeanProperty var sql: String,
     @BeanProperty var user: String,
     @BeanProperty var password: String,
     @BeanProperty var outputPath: String,
     @BeanProperty var outputFormat: String = "json",
     @BeanProperty var partition: Int,
     @BeanProperty var count: String,
     @BeanProperty var partCol: String,
     @BeanProperty var upperB: String,
     @BeanProperty var lowerB: String,
     @BeanProperty var load: Boolean,
     @BeanProperty var withDate: Boolean) {

    def this() = {
      this(null, null, null, null, null, null, null, null, 1, null, null, null, null, false, false)
    }

    def this(tableInfo: TableInfo) = {
      this(tableInfo.url, tableInfo.table, tableInfo.schema, tableInfo.sql, tableInfo.user, tableInfo.password, tableInfo.outputPath,
        tableInfo.outputFormat, tableInfo.partition, tableInfo.count, tableInfo.partCol, tableInfo.upperB, tableInfo.lowerB, tableInfo.getLoad, tableInfo.withDate)
    }
}

case class MergeInfo(
                      @BeanProperty var base: MergeTableInfo,
                      @BeanProperty var outputTable: TableInfo,
                      @BeanProperty var children: Array[MergeTableInfo] = Array.empty[MergeTableInfo]) {
  def this() = {
    this(null, null, Array.empty[MergeTableInfo])
  }
}

case class MergeTableInfo(
    @BeanProperty var table: TableInfo,
    @BeanProperty var mergeKey: String) {

  def this() = {
    this(null,"")
  }
}

//todo extract
object Configuration {
  def loadConfigurationFromYaml( file : File): TableList = {
    val input = new FileInputStream(file)
    val yaml = new Yaml(new Constructor(classOf[TableList]))
    val t = yaml.load(input).asInstanceOf[TableList]
    t
  }

  def generateYamlConf(list: TableList, fileName: String): Unit = {
    val yaml = new Yaml(new Constructor(classOf[TableList]))
    val writer = new FileWriter(fileName)
    yaml.dump(list, writer)
  }

  def generateConfigurationForDate(tableList: TableList, dates: Seq[String], duration: Duration): TableList = {
    var result = new ListBuffer[TableInfo]
    tableList.tables.filter(_.withDate).foreach(table => {
      dates.foreach(date => {
        val tableByDate = table.copy()
        val calculatedDates = calculateDate(date, duration)
        tableByDate.setOutputPath(table.getOutputPath.replace("$DATE", date))
        tableByDate.setSql(
          tableByDate
          .getSql
          .replace("$DATE", Date.valueOf(calculatedDates._1).toString)
          .replace("$TODATE", Date.valueOf(calculatedDates._2).toString))
        result += tableByDate
      })
    })
    TableList(result.toList.toArray)
  }

  private def calculateDate(date: String, duration: Duration)  = {
    val fromDate = LocalDate.parse(date).minusDays(1L)
    val toDate = fromDate.plusDays(duration.toDays)
    (fromDate, toDate)
  }

}
