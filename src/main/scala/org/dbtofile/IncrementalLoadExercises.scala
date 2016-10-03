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

import java.util.Properties

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SQLContext, SparkSession}
import org.dbtofile.conf.{DataSourceDefinition, DbTable}

import scala.language.implicitConversions

object IncrementalLoader {
  def main(args: Array[String]): Unit = {

    val dbUrl = "jdbc:mysql://192.168.99.100:32768/employees"
    val dbUser = "root"
    val dbPass = "root"


    val ds = DataSourceDefinition.load(dbUrl, dbUser, dbPass)

    val employeesTable = ds.tables.find(_.name == "employees").get

    val spark: SparkSession = SparkSession
      .builder()
      .appName("Incremental load")
      .master("local[4]")
      .config("spark.sql.warehouse.dir", sys.props("java.io.tmpdir"))
      .config("dbUrl", dbUrl)
      .config("dbUser", dbUser)
      .config("dbPassword", dbPass)
      .config("dbDriver", "com.mysql.jdbc.Driver")
      .config("snapshot.location", "snapshot")
      .config("partition.size", "100")
      //          .enableHiveSupport()
      .getOrCreate()

    val numOfPartitions = 100
    import org.dbtofile.SnapshotLoader._
    val employeesSnapshot = spark.load(employeesTable)
    val dbPartitions = employeesSnapshot.vPartitions(numOfPartitions)

    val historicalSnapshot = spark.loadParquet("snapshot", employeesTable)
    val historicalPartitions = historicalSnapshot.vPartitions(numOfPartitions)
      .withColumnRenamed("group_checksum", "historical_group_checksum")
      .withColumnRenamed("group_count", "historical_group_count")

    val partitionSummaries = historicalPartitions.join(dbPartitions, "group_key")
      .withColumn("require_reprocessing", when(col("group_checksum").equalTo(col("historical_group_checksum")), false).otherwise(true))

//    partitionSummaries.show(100, false)

    val partitionsToReprocess: Array[Long] = partitionSummaries.where(col("require_reprocessing")).collect().map(row => {
      val grpKey = row.getAs[Long]("group_key")
      val sourceChecksum = row.getAs[AnyRef]("group_checksum")
      val historicalChecksum = row.getAs[AnyRef]("historical_group_checksum")
      println(s"Partition $grpKey changed and will be reloaded: historical partition checksum - $historicalChecksum; actual partition checksum - $sourceChecksum")
      grpKey
    })


    val newSnapshot = historicalSnapshot.excludeVPartitions(partitionsToReprocess, numOfPartitions)
      .union(employeesSnapshot.loadVpartitions(partitionsToReprocess, numOfPartitions))
//    newSnapshot.show(1000, false)
    println(newSnapshot.count())
  }

}

object SnapshotLoader {
  implicit def loadSnapshot(spark: SparkSession): SnapshotLoader = new SnapshotLoader(spark)
}


class SnapshotLoader(val session: SparkSession) {
  def load(dataSource: DbTable) = new DataSnapshot(DataSource.jdbc(session), dataSource.name, dataSource.colNames, dataSource.pkNames, session)

  def loadParquet(path: String, dataSource: DbTable) = new DataSnapshot(DataSource.parquet(path), dataSource.name, dataSource.colNames, dataSource.pkNames, session)

}

class DataSnapshot(val dataSource: DataSource, val tableName: String, val columns: Array[String], val partitionKeys: Array[String], spark: SparkSession) {
  def vPartitions(numOfPartitions: Long): DataFrame = {
    val query = vPartitionQueryString(numOfPartitions)
    dataSource.read(query, tableName, spark.sqlContext)
  }

  //todo in future this logic can be moved to data source specific query builder
  def vPartitionQueryString(numOfPartitions: Long) =
  s"""
     |SELECT
     | ${checksum(partitionKeys)}%$numOfPartitions AS group_key,
     | SUM(${checksum(columns)}) AS group_checksum,
     | COUNT(*) as group_count
     | FROM $tableName
     | GROUP BY ${checksum(partitionKeys)}%$numOfPartitions
 """.stripMargin

  private[this] def checksum(columns: Array[String]) =
    s"""
       CRC32(CONCAT_WS('|',${columns.mkString(",")}))
     """

  def loadVpartitions(partitionIds: Array[Long], numOfPartitions:Long) = {
    val query =
      s"""
         |SELECT * FROM $tableName WHERE  ${checksum(partitionKeys)}%$numOfPartitions IN (${partitionIds.mkString(",")})
       """.stripMargin

    dataSource.read(query = query, tableName, spark.sqlContext)
  }

  def excludeVPartitions(partitionIds: Array[Long], numOfPartitions:Long) = {
    val query =
      s"""
         |SELECT * FROM $tableName WHERE  ${checksum(partitionKeys)}%$numOfPartitions NOT IN (${partitionIds.mkString(",")})
       """.stripMargin

    dataSource.read(query = query, tableName, spark.sqlContext)
  }
}

trait DataSource {
  def read(query: String, tableName: String, context: SQLContext): DataFrame
}

object DataSource {
  def jdbc(spark: SparkSession) = {
    val conf = spark.conf
    val props = new java.util.Properties
    if (conf.getOption("dbUser").isDefined) props.put("user", conf.get("dbUser"))
    if (conf.getOption("dbPassword").isDefined) props.put("password", conf.get("dbPassword"))
    if (conf.getOption("dbUrl").isDefined) props.put("url", conf.get("dbUrl"))
    if (conf.getOption("dbDriver").isDefined) props.put("driver", conf.get("dbDriver"))
    new JdbcDataSource(conf.get("dbUrl"), conf.get("dbDriver"), props)
  }

  def parquet(path: String) = new ParquetDataSource(path)
}

class JdbcDataSource(val connectionString: String, val driver: String, val connectionProperties: Properties) extends DataSource {
  def read(query: String, tableName: String, context: SQLContext): DataFrame = {
    val queryString: String = s"($query) as $tableName".replaceAll("( |\n)+", " ")
    println(queryString)
    context.read.jdbc(connectionString, queryString, connectionProperties)
  }
}

class ParquetDataSource(val path: String) extends DataSource {
  override def read(query: String, tableName: String, context: SQLContext): DataFrame = {
    getOrCreateTempView(tableName, context)
    context.sql(query)
  }

  def getOrCreateTempView(tableName: String, context: SQLContext): Unit = {
    val tables: Array[String] = context.tableNames()
    if (!tables.contains(tableName)) {
      println(s"New temporary view '$tableName' will be created and tied to active SparkSession")
      val df = context.read.parquet(s"$path/$tableName")
      df.createOrReplaceTempView(tableName)
    }
  }
}