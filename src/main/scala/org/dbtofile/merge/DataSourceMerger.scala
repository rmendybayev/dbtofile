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

package org.dbtofile.merge

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.dbtofile.conf.MergeInfo

import scala.collection.mutable
import scala.util.Try
import scala.math.{Integral, Numeric, Ordering}
import scala.reflect.runtime.universe.typeTag




/**
  * Created by grinia on 8/16/16.
  */
@Deprecated
object DataSourceMerger {


  class Converter {

  }

  def sfTypeToScalaType(dataType: DataType, value:Any) : Try[Any] = dataType match {

    case DataTypes.IntegerType => Try(value.asInstanceOf[Number])
    case DataTypes.StringType => Try(value.asInstanceOf[String])
    case DataTypes.ShortType=> Try(value.asInstanceOf[Number])
    case DataTypes.LongType => Try(value.asInstanceOf[Number])
    case DataTypes.FloatType => Try(value.asInstanceOf[Number])
    case DataTypes.DoubleType => Try(value.asInstanceOf[Number])

  }

  def mergeTable(joinInfo:MergeInfo, sqlContext: SQLContext): DataFrame = ???
}
