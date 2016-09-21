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

import org.dbtofile.helpers.{EmbeddedH2, EmbeddedMySql}
import org.scalatest.{BeforeAndAfterAll, FunSuite, Matchers}


trait DdToFileSuite extends FunSuite with Matchers with BeforeAndAfterAll {
  val dbPort: Int = 32769
  val dbUser: String = "db_user"
  val dbPass: String = "pass"
  val employeesDb: String = "employees"

//  override def convertToLegacyEqualizer[T](left: T): LegacyEqualizer[T] = new LegacyEqualizer(left)
//  override def convertToLegacyCheckingEqualizer[T](left: T): LegacyCheckingEqualizer[T] = new LegacyCheckingEqualizer(left)

  def  db = new EmbeddedH2

  def withEmbeddedDb(user: String, password: String, initScript: String, dbName: String)(test: => Unit) = {
//    val db = new EmbeddedMySql(port = port, dbUser = user, password = password)

    db.start
    db.createDb(dbName)
    db.importData(db = Option(dbName),dataScript = initScript)
    try {
      test
    } finally {
      println("Embedded database server will be stopped")
      db.stop
    }
  }

  def withEmployeesDb(user: String = dbUser, password: String = dbPass) =
    withEmbeddedDb(
      user = user,
      password = password,
      initScript = getClass.getResource("/employees_db.sql.zip").getFile,
      dbName = employeesDb) _
}

