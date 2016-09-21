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

package org.dbtofile.utils

import java.sql.DatabaseMetaData

import org.dbtofile.conf.{Column, DbTable, Relation}

object DbOps {

  type TableWithReferences = (DbTable, List[Relation])

  def listTables(metadata: DatabaseMetaData) = {

    val catalog: String = metadata.getConnection.getCatalog
    val schemaPattern: String = "public"
    val tableNamePattern: String = "%"
    val types: Array[String] = Array("TABLE")

    val tablesRs = metadata.getTables(catalog, schemaPattern, tableNamePattern, types)
    (for ((_, r) <- Iterator.continually((tablesRs.next(), tablesRs)).takeWhile(_._1)) yield {
      r.getString(3)
    }).toList
  }

  def listColumns(metadata: DatabaseMetaData, table: String) = {
    val columnsRs = metadata.getColumns(metadata.getConnection.getCatalog, null, table, null)
    (for ((_, r) <- Iterator.continually((columnsRs.next(), columnsRs)).takeWhile(_._1)) yield {
      val columnName = r.getString(4)
      val columnType = r.getInt(5)
      Column(columnName, columnType)
    }).toList
  }

  def primaryKey(metadata: DatabaseMetaData, tableName: String) = {
    val pkRs = metadata.getPrimaryKeys(null, null, tableName)

    (for ((_, r) <- Iterator.continually((pkRs.next(), pkRs)).takeWhile(_._1)) yield {
      val columnName = r.getString(4)
      val columnType = r.getInt(5)
      Column(columnName, columnType)
    }).toList

  }

  def relations(metadata: DatabaseMetaData, tableName: String) = {
    val fkRs = metadata.getImportedKeys(null, null, tableName)
    (for ((_, r) <- Iterator.continually((fkRs.next(), fkRs)).takeWhile(_._1)) yield {
      val fkTableName = r.getString("FKTABLE_NAME")
      val fkColumnName = r.getString("FKCOLUMN_NAME")
      val pkTableName = r.getString("PKTABLE_NAME")
      val pkColumnName = r.getString("PKCOLUMN_NAME")
      Relation(pkTableName, pkColumnName, fkTableName, fkColumnName)
    }).toList

  }
}