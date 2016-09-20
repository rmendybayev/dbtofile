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

package org.dbtofile.helpers

import java.io.File
import java.sql.{Connection, DriverManager}
import java.util.Scanner

import com.wix.mysql.EmbeddedMysql
import com.wix.mysql.config.{Charset, MysqldConfig, SchemaConfig}
import com.wix.mysql.distribution.Version

import scala.util.{Failure, Try}

trait EmbeddedDb {

  def start

  def stop

  def createDb(dbName: String):Unit = {
    val conn = jdbcConnection(None)
    try conn.createStatement().executeUpdate(s"CREATE DATABASE $dbName") finally if (conn != null) conn.close()
  }

  def importData(db: Option[String] = None, dataScript: String) = {
    val sql = if (dataScript.endsWith("zip")) FilesOps.unzipToTmp(dataScript) else List(dataScript)
    val connection: Connection = jdbcConnection(db)
    try {
      val runner = new ScriptRunner(connection)
      sql.foreach(script => runner.executeScript(new File(script)))
    } finally {
      if (connection != null) connection.close()
    }

  }

  def jdbcConnection(db: Option[String]): Connection = DriverManager.getConnection(connectionString(db))

  def connectionString(dbName: Option[String]): String
}

/**
  * TODO : fixme
  * Embedded MySql cannot be started on Windows 8:  Cannot open Windows EventLog; check privileges, or start server with --log_syslog=0
  */
class EmbeddedMySql(val host: String = "localhost",
                    val port: Int = 32777,
                    val dbUser: String = "dbUser",
                    val password: String = "") extends EmbeddedDb {
  val conf = MysqldConfig.aMysqldConfig(Version.v5_7_latest)
    .withCharset(Charset.UTF8)
    .withPort(port)
    .withUser(dbUser, password)
    .build()

  private[this] var server: EmbeddedMysql = _

  override def start: Unit = {
    if (server == null) server = EmbeddedMysql.anEmbeddedMysql(conf).start()
  }

  override def stop: Unit = if (server != null) server.stop()

  override def connectionString(dbName: Option[String]): String = dbName match {
    case Some(db) => s"jdbc:mysql://$host:$port/$db"
    case None => s"jdbc:mysql://$host:$port"
  }

  override def jdbcConnection(db: Option[String]): Connection = DriverManager.getConnection(connectionString(db), dbUser, password)
}

class EmbeddedH2() extends EmbeddedDb {
  Class.forName("org.h2.Driver")

  private [this] val dbFilesLocation = FilesOps.TMPDIR.toAbsolutePath

  private[this] val server = org.h2.tools.Server.createTcpServer()

  override def start: Unit = if (server != null) server.start()

  override def stop: Unit = {
    if(dbFilesLocation.toFile.exists()) dbFilesLocation.toFile.delete()
    if (server != null && server.isRunning(true)) server.stop()
  }

  override def connectionString(dbName: Option[String]): String = dbName match {
    case Some(db) => s"jdbc:h2:${dbFilesLocation}/$db;MODE=MySQL"
  }


  override def createDb(dbName: String): Unit = {
    println("H2 : Skip create db operation. By default, if the database specified in the URL does not yet exist, " +
      "a new (empty) database is created automatically. " +
      "The user that created the database automatically becomes the administrator of this database.")
    //
  }

  override def jdbcConnection(db: Option[String]): Connection = DriverManager.getConnection(connectionString(db), "sa", "")
}


class ScriptRunner(val connection: Connection, val stopOnError: Boolean = true) {
  def executeScript(script: File, delimiter: String = ";") = {
    val scanner: Scanner = new Scanner(script).useDelimiter(delimiter)
    while (scanner.hasNext) {
      val query: String = scanner.next()
      if (query.trim.nonEmpty) {
        println(s"${query.take(100)} ${if (query.length > 100) "..."}")
        Try(connection.createStatement().executeUpdate(query)) match {
          case Failure(ex) if stopOnError => throw ex
          case _ => println("OK")
        }
      }

    }

  }
}
