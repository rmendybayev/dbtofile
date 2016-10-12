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

package org.dbtofile.conf

import java.sql.{DatabaseMetaData, DriverManager}

import org.apache.spark.sql.SparkSession
import org.dbtofile.utils.{DbOps, YamlOps}

import scala.beans.BeanProperty


case class Db(@BeanProperty dbName: String,
              @BeanProperty tables: Array[DbTable],
              @BeanProperty relations: Array[Relation]) {

  /**
    * List tables that can be joined to specified table
    *
    * @param table
    * @return
    */
  def childTables(table: DbTable): Array[DbTable] = childTables(table.name)

  def childTables(tableName: String): Array[DbTable] = relations
    .filter(_.baseTable == tableName)
    .flatMap(r => tables.find(_.name == r.childTable))

}

/**
  *
  * @param name
  * @param dataType java.sql.Types
  */
case class Column(@BeanProperty name: String,
                  @BeanProperty dataType: Int)

case class DbTable(@BeanProperty name: String,
                   @BeanProperty pk: Array[Column],
                   @BeanProperty columns: Array[Column]) {

  lazy val pkNames: Array[String] = pk.map(_.name)

  lazy val colNames: Array[String] = columns.map(_.name)
}

case class Relation(@BeanProperty baseTable: String,
                    @BeanProperty basePK: String,
                    @BeanProperty childTable: String,
                    @BeanProperty childFK: String)

object DataSourceDefinition {
  def load(connectionString: String, user: String, password: String): Db = {
    val connection = DriverManager.getConnection(connectionString, user, password)
    val metadata: DatabaseMetaData = connection.getMetaData

    val tableNames: List[String] = DbOps.listTables(metadata)
    val tables: List[(DbTable, List[Relation])] = tableNames.map(tableName => {
      val columns: Array[Column] = DbOps.listColumns(metadata, tableName).toArray
      val pk = DbOps.primaryKey(metadata, tableName).toArray
      val tableRelations = DbOps.relations(metadata, tableName)
      (DbTable(tableName, pk, columns), tableRelations)
    })

    Db(connection.getCatalog, tables.map(_._1).toArray, tables.flatMap(_._2).toArray)


  }
}