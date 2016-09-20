/*
 *
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
 *
 */

package org.dbtofile


import org.scalatest.Tag

class ExecutionEnvironmentTest extends DdToFileSuite {


  test("check test environment initialization", Tag("slow")) {

    withEmployeesDb() {
      val conn = db.jdbcConnection(Some("employees"))
      try {
        val metadata = conn.getMetaData

        val catalog: String = conn.getCatalog
        val schemaPattern: String = null
        val tableNamePattern: String = null
        val types: Array[String] = null

        val tablesRs = metadata.getTables(catalog, schemaPattern, tableNamePattern, types)

        val tables = (for ((_, r) <- Iterator.continually((tablesRs.next(), tablesRs)).takeWhile(_._1)) yield {
          (r.getString(2), r.getString(3))
        }).toList.filter(_._1 == "public").map(_._2)

        tables.foreach(table => {
          println(s"TABLE : $table")
          val foreignKeys = metadata.getImportedKeys(catalog, null, table)
          while (foreignKeys.next()) {
            val fkTableName = foreignKeys.getString("FKTABLE_NAME")
            val fkColumnName = foreignKeys.getString("FKCOLUMN_NAME")
            val pkTableName = foreignKeys.getString("PKTABLE_NAME")
            val pkColumnName = foreignKeys.getString("PKCOLUMN_NAME")
            println(fkTableName + "." + fkColumnName + " -> " + pkTableName + "." + pkColumnName)
          }
        })

        val stmt = conn.createStatement()
        val rs = stmt.executeQuery("SELECT COUNT(*) AS total FROM employees")
        rs.next()
        rs.getInt("total") should be(300024)
      } finally {
        conn.close()
      }

    }
  }
}
