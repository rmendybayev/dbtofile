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

package org.dbtofile

import java.io.File

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import org.dbtofile.conf.{DataSourceDefinition, DbTable}

object IncrementalLoadExercises {


  def main(args: Array[String]): Unit = {

    val dbUrl = "jdbc:mysql://192.168.99.100:32783/employees"
    val dbUser = "root"
    val dbPass = "root"

    val opts = Map(
      "url" -> dbUrl,
      "user" -> dbUser,
      "password" -> dbPass,
      "driver" -> "com.mysql.jdbc.Driver",
      "snapshot.location" -> "snapshot",
      "partition.size" -> "100"
    )

    val ds = DataSourceDefinition.load(dbUrl, dbUser, dbPass)

    val employeesTable = ds.tables.find(_.name=="employees").get
    val loader = new Loader(opts)
    loader.load(employeesTable)


  }

}

class Loader(val options:Map[String,String]) {

  val spark = SparkSession
    .builder()
    .appName("Incremental load")
    .master("local[4]")
    .config("spark.sql.warehouse.dir", "file:///c:/tmp/spark-warehouse")
    //          .enableHiveSupport()
    .getOrCreate()


  def load(tableInfo: DbTable, addChecksum: Boolean = true) = {

    val snapshot= new File(s"${options("snapshot.location")}/${tableInfo.name}")
    val df = if(snapshot.exists()){
      loadIncrement(tableInfo, addChecksum)
    }else{
      initialLoad(tableInfo, addChecksum)
    }

  }

  def initialLoad (tableInfo: DbTable, addChecksum: Boolean = true) = {
    spark.read.format("jdbc").options(options).option("dbtable", tableInfo.name).load()
      .write.parquet(s"${options("snapshot.location")}/${tableInfo.name}")

  }

  def loadIncrement(tableInfo: DbTable, addChecksum: Boolean = true) = {
    //"dbtable" -> "employees",

    val recordsPerPartition=options.getOrElse("partition.size","10000").toInt
    val table: DataFrame = spark.read.format("jdbc").options(options).option("dbtable", tableInfo.name).load()
    val snapshot:DataFrame = spark.read.parquet(s"${options("snapshot.location")}/${tableInfo.name}")

    val numOfPartitions = table.count()/recordsPerPartition


    val tablePartitions: DataFrame = table.select(
      groupKeyColumn(tableInfo.pk.map(_.name).toList,numOfPartitions),
      rowChecksumColumn(tableInfo.columns.map(_.name).toList)
    ).groupBy("group_key").agg(sum("checksum").as("group_checksum"), count("*").as("group_count"))


    val snapshotPartitions = snapshot.select(
      groupKeyColumn(tableInfo.pk.map(_.name).toList,numOfPartitions),
      rowChecksumColumn(tableInfo.columns.map(_.name).toList)
    ).groupBy("group_key").agg(sum("checksum").as("snpsht_group_checksum"), count("*").as("snpsht_group_count"))

    val partitions = tablePartitions.join(snapshotPartitions, "group_key").withColumn("require_reprocessing",
      when(col("group_checksum").equalTo(col("snpsht_group_checksum")), false).otherwise(true))

    partitions.show(100)


  }

  def groupKeyColumn(keys: List[String], numOfPartitions: Long = 10, columnName:String="group_key"): Column = {
    pmod(crc32(concat_ws("|", keys.map(c => col(c)): _*)), lit(numOfPartitions)).as(columnName)
  }

  def rowChecksumColumn(columns: List[String]): Column = crc32(concat_ws("|", columns.map(c => col(c)): _*)).as("checksum")

}

