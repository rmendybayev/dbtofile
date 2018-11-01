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

import java.io.FileInputStream

import com.typesafe.config.ConfigFactory
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.yaml.snakeyaml.Yaml
import org.yaml.snakeyaml.constructor.Constructor

/**
  * Created by grinia on 8/16/16.
  */
object TableImport {

  def main(args: Array[String]): Unit = {
    val appConf = ConfigFactory.load()
    val sparkConf = new SparkConf()
      .setAppName(appConf.getString("app.name"))

    val spark = SparkSession
      .builder()
      .config(sparkConf)
      .getOrCreate()

    val input = new FileInputStream(appConf.getString("conf"))
    val yaml = new Yaml(new Constructor(classOf[org.dbtofile.conf.TableList]))
    val t = yaml.load(input).asInstanceOf[org.dbtofile.conf.TableList]

    for (table <- t.tables) {
//      DataLoader.loadData(table, spark)
      println(table.table)
    }
}
}
