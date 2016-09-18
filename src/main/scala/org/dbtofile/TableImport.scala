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

package org.dbtofile

import java.io.{File, FileInputStream}

import org.apache.spark.sql.{DataFrame, SaveMode}
import org.apache.spark.{SparkConf, SparkContext}
import org.dbtofile.conf.TableList
import org.dbtofile.join.DataSourceMerger
import org.dbtofile.load.DataLoader
import org.yaml.snakeyaml.Yaml
import org.yaml.snakeyaml.constructor.Constructor

/**
  * Created by grinia on 8/16/16.
  */
object TableImport {

  def main(args: Array[String]): Unit = {
    case class Config( conf: File = new File("."), loadTables: Boolean = false)
    val parser = new scopt.OptionParser[Config]("scopt") {
      head("TableImport", "0.1")

      opt[File]('c', "conf").required().valueName("").
        action( (x, c) => c.copy(conf = x) ).
        text("conf is a required configuration property")
    }

    parser.parse(args, Config()) match {
      case Some(config) =>
      // do stuff

        val conf = new SparkConf

        val sc = new SparkContext("local[*]", "TableImport", conf)
        val sqlContext = new org.apache.spark.sql.SQLContext(sc)

//        val filename = "src/main/resources/tables_conf.yaml"
        val input = new FileInputStream(config.conf)
        val yaml = new Yaml(new Constructor(classOf[TableList]))
        val t = yaml.load(input).asInstanceOf[TableList]

        for (table <- t.tables) {
          DataLoader.loadDataFromMySQL(table, sqlContext)
        }

        for (join <- t.joins) {
          val df: DataFrame = DataSourceMerger.joinTable(join, sqlContext)
          df.write.mode(SaveMode.Overwrite).format(join.outputTable.outputFormat).save(join.outputTable.outputPath)
        }

      case None =>
      // arguments are bad, error message will have been displayed
    }

  }
}
