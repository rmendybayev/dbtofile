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

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.dbtofile.conf.{DataSourceDefinition, TableList}
import org.dbtofile.load.DataLoader
import org.dbtofile.merge.DataSourceMerger
import org.dbtofile.utils.YamlOps
import org.yaml.snakeyaml.Yaml
import org.yaml.snakeyaml.constructor.Constructor

/**
  * Created by grinia on 8/16/16.
  */
object TableImport {

  def main(args: Array[String]): Unit = {
    case class Config( conf: File = new File("."), loadTables: Boolean = false,
                       generateConf: Boolean = false,
                       mergeTables: Boolean = false,
                       dbUrl: Option[String] = Option.empty[String],
                       dbUsername: Option[String] = Option.empty[String],
                       dbPassword: Option[String] = Option.empty[String],
                       dbConfOutput: Option[File] = Option.empty[File])
    val parser = new scopt.OptionParser[Config]("scopt") {
      head("TableImport", "0.1")


      cmd("load").action( (_, c) => c.copy(loadTables = true) ).
        text("load command.").
        children(
          opt[File]('c', "conf").required().valueName("").
            action( (x, c) => c.copy(conf = x) ).
            text("conf is a required configuration property")
        )
      cmd("merge").action( (_, c) => c.copy(mergeTables = true) ).
        text("merge command.").
        children(
          opt[File]('c', "conf").required().valueName("").
            action( (x, c) => c.copy(conf = x) ).
            text("conf is a required configuration property")
        )
      cmd("generate").action( (_, c) => c.copy(generateConf = true) ).
        text("generate command.").
        children(
          opt[String]('d',"db-url").required().action( (x, c) =>
            c.copy(dbUrl = Option(x)) ).text("External Database URL"),
          opt[String]('u',"db-username").required().action( (x, c) =>
            c.copy(dbUsername = Option(x)) ).text("External Database Username"),
          opt[String]('p',"db-password").required().action( (x, c) =>
            c.copy(dbPassword = Option(x)) ).text("External Database Password"),
          opt[String]('f',"output-file").action( (x, c) =>
            c.copy(dbConfOutput = Option(new File(x))) ).text("Output file for generated file")
        )
    }

    parser.parse(args, Config()) match {
      case Some(config) =>
          if (config.loadTables) {
            val spark = SparkSession
              .builder()
              .master("local[*]")
              .appName("TableImport")
              .getOrCreate()


            //        val filename = "src/main/resources/tables_conf.yaml"
            val input = new FileInputStream(config.conf)
            val yaml = new Yaml(new Constructor(classOf[TableList]))
            val t = yaml.load(input).asInstanceOf[TableList]

            for (table <- t.tables) {
              DataLoader.loadDataFromMySQL(table, spark)
            }
          }
          if (config.mergeTables) {
            val spark = SparkSession
              .builder()
              .master("local[*]")
              .appName("TableImport")
              .getOrCreate()

            //        val filename = "src/main/resources/tables_conf.yaml"
            val input = new FileInputStream(config.conf)
            val yaml = new Yaml(new Constructor(classOf[TableList]))
            val t = yaml.load(input).asInstanceOf[TableList]

            for (merge <- t.merges) {
              var df = DataSourceMerger.mergeTable(merge, spark)
              df.write.mode(SaveMode.Overwrite).format(merge.outputTable.outputFormat).save(merge.outputTable.outputPath)
            }
          }
          if (config.generateConf) {
            Option.empty.isDefined
            if (config.dbConfOutput.isDefined) {
              YamlOps.toString(
                        DataSourceDefinition.load(config.dbUrl.get, config.dbUsername.get, config.dbPassword.get),
                        config.dbConfOutput.get)
            } else {
              println(YamlOps.toString(
                DataSourceDefinition.load(config.dbUrl.get, config.dbUsername.get, config.dbPassword.get)))
            }

          }

      case None =>
      // arguments are bad, error message will have been displayed
    }

  }
}
