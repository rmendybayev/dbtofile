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

package org.dbtofile.sandbox

import java.util.Properties

import org.apache.spark.sql.SparkSession.Builder
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.dbtofile.conf.{DataSourceDefinition, DbTable}


object AppDriver {


  def main(args: Array[String]): Unit = {
    val dbUrl = "jdbc:mysql://192.168.99.100:32773/employees"
    val dbUser = "root"
    val dbPass = "root"
    val ds = DataSourceDefinition.load(dbUrl, dbUser, dbPass)

    //spark configs can be used to distribute application settings
    val sessionBuilder: Builder = SparkSession
      .builder()
      .appName("Incremental load")
      .master("local[*]")
      .config("spark.sql.warehouse.dir", sys.props("java.io.tmpdir"))
      .config("dbUrl", dbUrl)
      .config("dbUser", dbUser)
      .config("dbPassword", dbPass)
      .config("dbDriver", "com.mysql.jdbc.Driver")
      .config("snapshot.location", "snapshot")
      .config("partition.size", "100")
    //          .enableHiveSupport()

    //register tables or load configs to spark session
    ds.tables.foreach(t => register(t, sessionBuilder))

    val session: SparkSession = sessionBuilder.getOrCreate()

    //demonstration
    val employeesTable = ds.tables.find(_.name == "employees").get

    import org.dbtofile.sandbox.SnapshotOps._

    val activeData = session.dataSnapshot("employees").loadFromDb.partitionedByPK
    val historicalData = session.dataSnapshot("employees").loadFromParquet.partitionBy("emp_no")

    val diff = historicalData.compareWith(activeData, "active")
    val partitionsToReprocess: Array[Long] = diff.where(col("was_changed")).collect().map(row => {
      val grpKey = row.getAs[Long]("group_key")
      val historicalChcksm = row.getAs[AnyRef]("group_checksum")
      val activeChcksum = row.getAs[AnyRef]("active_group_checksum")
      val active_group_count = row.getAs[Long]("active_group_count")
      println(
        s"""Partition #$grpKey was changed:
            |active partition checksum     - $activeChcksum
            |historical partition checksum - $historicalChcksm""".stripMargin)
      println(s"$active_group_count records will be reloaded")
      grpKey
    })

    //create new snapshot
    val updates = activeData.loadPartitions(partitionsToReprocess)
    val withoutChanges = historicalData.loadOtherPartitions(partitionsToReprocess)

    println(s"UPDATED : ${updates.count()} records")
    println(s"WITHOUT CHANGES : ${withoutChanges.count()} records")
  }

  def register(table: DbTable, sb: Builder): Builder = sb
    .config(s"${table.name}.isActive", true)
    .config(s"${table.name}.columns", table.colNames.mkString(","))
    .config(s"${table.name}.keys", table.pkNames.mkString(","))
    .config(s"${table.name}.numberOfPartitions", 100)
    .config(s"${table.name}.snapshot.location", s"snapshot/${table.name}")
}


class SnapshotBuilder(session: SparkSession, name: Option[String] = None) {
  def dataSnapshot(name: String): SnapshotBuilder = {
    require(session.conf.getOption(s"$name.isActive").isDefined, s"Datasource '$name' is not defined")
    new SnapshotBuilder(session, Option(name))
  }

  def loadFromDb: Snapshot = new RdbSnapshot(name = name.get, session)

  def loadFromParquet: Snapshot = new ParquetSnapshot(name.get, session)
}

object SnapshotOps {
  implicit def fromSession(session: SparkSession): SnapshotBuilder = new SnapshotBuilder(session)
}

trait Snapshot {
  val name: String
  val columns: Array[String]
  val pks: Array[String] = Array.empty

  def partitionBy(partitionKey: String, partitionKeys: String*) = {
    //todo inject
    val numOfPartitions: Long = 100
    new PartitionedSnapshot(partitionKey +: partitionKeys.toArray, numOfPartitions, this)
  }

  def partitionedByPK = {
    val numOfPartitions: Long = 100
    require(pks.nonEmpty, "PK columns is not defined for $name")
    new PartitionedSnapshot(pks, numOfPartitions, this)
  }

  def load(query: String): DataFrame

  //todo override this method to get DataSource specific query builder
  def queryBuilder = new DefaultQueryBuilder
}

class PartitionedSnapshot(val partitionKey: Array[String], val numberOfPartitions: Long, val snapshot: Snapshot) extends Snapshot {

  override val name: String = snapshot.name
  override val columns: Array[String] = snapshot.columns

  lazy val partitionSummary: DataFrame = {
    val query = queryBuilder.describePartitionsQueryString(name, columns, partitionKey, numberOfPartitions)
    load(query)
  }

  def loadPartitions(partitionIdx: Array[Long]): DataFrame = loadPartitions(partitionIdx, numberOfPartitions)

  def loadPartitions(partitionIdx: Array[Long], numOfPartitions: Long): DataFrame = load(
    snapshot.queryBuilder.selectPartitionsQueryString(
      table = name,
      columns = columns,
      partitionKeys = partitionKey,
      numOfPartitions = numOfPartitions,
      partitionIds = partitionIdx
    ))

  def loadOtherPartitions(partitionIdxToExclude: Array[Long]): DataFrame = loadOtherPartitions(partitionIdxToExclude, numberOfPartitions)

  def loadOtherPartitions(partitionIdxToExclude: Array[Long], numOfPartitions: Long): DataFrame = load(
    snapshot.queryBuilder.selectOtherPartitionsQueryString(
      table = name,
      columns = columns,
      partitionKeys = partitionKey,
      numOfPartitions = numOfPartitions,
      partitionIds = partitionIdxToExclude
    ))

  override def load(query: String): DataFrame = snapshot.load(query)

  def compareWith(another: PartitionedSnapshot, anotherAlias: String = "another"): DataFrame = {

    require(this.name == another.name, "different snapshot cannot be compared")
    require(this.numberOfPartitions == another.numberOfPartitions, "number of partitions should be the same")
    require(this.partitionKey.sameElements(another.partitionKey), "partition keys should be the same")

    val thisPartitions = this.partitionSummary
    val anotherPartitions = another.partitionSummary.withColumnRenamed("group_checksum", s"${anotherAlias}_group_checksum")
      .withColumnRenamed("group_count", s"${anotherAlias}_group_count")

    //both group should contain the same groups
    thisPartitions.join(anotherPartitions, "group_key")
      .withColumn("was_changed", when(col("group_checksum").equalTo(col(s"${anotherAlias}_group_checksum")), false).otherwise(true))
  }


}

class RdbSnapshot(val name: String, val session: SparkSession) extends Snapshot {


  override val pks: Array[String] = session.conf.getOption(s"$name.keys").map(_.split(",")).getOrElse(Array.empty)
  override val columns: Array[String] = session.conf.get(s"$name.columns").split(",")

  lazy val connectionString: String = session.conf.get("dbUrl")

  private[this] lazy val connectionProperties = {
    val props = new Properties()
    val conf = session.conf
    if (conf.getOption("dbUser").isDefined) props.put("user", conf.get("dbUser"))
    if (conf.getOption("dbPassword").isDefined) props.put("password", conf.get("dbPassword"))
    if (conf.getOption("dbUrl").isDefined) props.put("url", conf.get("dbUrl"))
    if (conf.getOption("dbDriver").isDefined) props.put("driver", conf.get("dbDriver"))
    props
  }


  override def load(query: String): DataFrame = {
    val _query = s"($query) as $name"
    println(_query)
    session.read.jdbc(connectionString, _query, connectionProperties)
  }
}

class ParquetSnapshot(val name: String, val session: SparkSession) extends Snapshot {
  override val pks: Array[String] = session.conf.getOption(s"$name.keys").map(_.split(",")).getOrElse(Array.empty)
  override val columns: Array[String] = session.conf.get(s"$name.columns").split(",")

  val baseDir = session.conf.get(s"$name.snapshot.location")

  override def load(query: String): DataFrame = {
    session.read.parquet(baseDir).createOrReplaceTempView(name)
    session.sqlContext.sql(query)
  }
}

class DefaultQueryBuilder {
  //todo add Relational DB specific implementations
  def selectPartitionsQueryString(table: String, columns: Array[String], partitionKeys: Array[String], numOfPartitions: Long, partitionIds: Array[Long]) = queryInLine(
    s"""
       |SELECT * FROM $table WHERE  ${checksum(partitionKeys)}%$numOfPartitions IN (${partitionIds.mkString(",")})
     """.stripMargin
  )

  def selectOtherPartitionsQueryString(table: String, columns: Array[String], partitionKeys: Array[String], numOfPartitions: Long, partitionIds: Array[Long]) = queryInLine(
    s"""
       |SELECT * FROM $table WHERE  ${checksum(partitionKeys)}%$numOfPartitions NOT IN (${partitionIds.mkString(",")})
     """.stripMargin
  )

  def describePartitionsQueryString(table: String, columns: Array[String], partitionKeys: Array[String], numOfPartitions: Long) = queryInLine(
    s"""
       |SELECT
       | ${checksum(partitionKeys)}%$numOfPartitions AS group_key,
       | SUM(${checksum(columns)}) AS group_checksum,
       | COUNT(*) as group_count
       | FROM $table
       | GROUP BY ${checksum(partitionKeys)}%$numOfPartitions
 """.stripMargin
  )

  private[this] def checksum(columns: Array[String]) =
    s"""
       CRC32(CONCAT_WS('|',${columns.mkString(",")}))
     """

  private[this] def queryInLine(queryString: String) = queryString.replaceAll("( |\n)+", " ")
}


