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


import java.sql.DriverManager

import org.scalatest.{FunSuite, Matchers, Tag}

class ExecutionEnvironmentTest extends FunSuite with DdToFileTestSuite with Matchers {

  test("create and populate embedded Db", Tag("slow")) {
    val port: Int = 32769
    val user: String = "db_user"
    val pass: String = "pass"
    withMySql(port, user, pass, getClass.getResource("/employees_db.sql.zip").getFile) {
      val conn = DriverManager.getConnection(s"jdbc:mysql://localhost:$port/employees", user, pass)

      val stmt = conn.createStatement()
      val rs = stmt.executeQuery("SELECT COUNT(*) AS total FROM employees.employees")
      rs.next()
      val count = rs.getInt("total")

      count should be(300024)
      conn.close()

    }
  }
}
