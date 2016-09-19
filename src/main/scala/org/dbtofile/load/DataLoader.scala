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

import org.apache.spark.sql.{SQLContext, SaveMode, SparkSession}
import org.dbtofile.conf.TableInfo


/**
  * Created by grinia on 8/16/16.
  */
object DataLoader {

    def loadDataFromMySQL(dbInfo: TableInfo, sqlContext: SparkSession,
                          outputPath: String = null, outputFormat:String = null ) {

      if (!dbInfo.load) {
        return
      }
      var table = sqlContext.read.format("jdbc")
        .option("url", dbInfo.url)
        .option("dbtable", dbInfo.table)
        .option("user", dbInfo.user)
        .option("password", dbInfo.password)
        .load()
      table.createOrReplaceTempView(dbInfo.table)

      table.write.mode(SaveMode.Append)
                .format(Option(outputFormat).getOrElse(dbInfo.outputFormat))
                .save(Option(outputFormat).getOrElse(dbInfo.outputPath))
    }
}
