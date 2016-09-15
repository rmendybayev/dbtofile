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

package org.dbtofile.join

import org.apache.spark.sql.types.ArrayType
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.dbtofile.conf.JoinInfo

import scala.collection.mutable


/**
  * Created by grinia on 8/16/16.
  */
object DataSourceMerger {

  def joinTable(joinInfo:JoinInfo, sqlContext: SQLContext): DataFrame = {
    var base = sqlContext.read.load(joinInfo.base.table.outputPath)

    var childrens = joinInfo.children.map {
      info => (info, sqlContext.read.load(info.table.outputPath))
    }

    var mixedBaseSchema = base.schema
    for (child <- childrens) {
      val (info, df) = child
      mixedBaseSchema = mixedBaseSchema.add(info.getTable.table, ArrayType(df.schema))
    }

    mixedBaseSchema.printTreeString()

    var grouppedChildByKey = childrens.map { case (info, df) => df.map { r: Row => r.getAs[Any](info.joinKey) -> r }
        .groupByKey.map[(Any, (String, Seq[Row]))] {
        case ((key: Any, value: Seq[Row])) => key -> (info.table.table -> value)
      }
    }

    var baseDS = base.map { r => r.getAs[Any](joinInfo.base.joinKey) -> ("base" -> Seq(r)) }
    var mixedBase = Seq(baseDS)
    mixedBase ++= grouppedChildByKey
    var joinedRdds = sqlContext.sparkContext.union[(Any, (String, Seq[Row]))](mixedBase)


    var finalDF = joinedRdds.groupByKey.flatMap[Row] { case ((key: Any, value: Iterable[(String, Seq[Row])])) =>
      var map = value.toMap
      var b = map.get("base")
      var children = map.filter(p => !p._1.equals("base")).values
      if (b.isDefined) {
        var a = mutable.MutableList.newBuilder[Any]
        a ++= b.get.head.toSeq
        for (child <- children) {
          a += child.toSeq
        }
        Seq(Row.fromSeq(a.result()))
      } else {
        Seq.empty
      }
    }

    var dfToReturn = sqlContext.createDataFrame(finalDF, mixedBaseSchema)
    return dfToReturn;
  }
}
