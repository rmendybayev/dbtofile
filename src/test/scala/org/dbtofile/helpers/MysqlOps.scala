package org.dbtofile.helpers

import java.io.File
import java.sql.{Connection, DriverManager}
import java.util.Scanner

import com.wix.mysql.EmbeddedMysql
import com.wix.mysql.config.{Charset, MysqldConfig, SchemaConfig}
import com.wix.mysql.distribution.Version

import scala.util.{Failure, Try}

object MysqlOps {

   def embeddedDb(port: Int,
                  userName: String,
                  password: String
              ): EmbeddedMysql = {
     val conf = MysqldConfig.aMysqldConfig(Version.v5_7_latest)
       .withCharset(Charset.UTF8)
       .withPort(port)
       .withUser(userName, password).
       build()
     EmbeddedMysql.anEmbeddedMysql(conf).start()
   }

  def createDatabase(db:EmbeddedMysql, dbName:String): Unit ={
    db.addSchema(SchemaConfig.aSchemaConfig(dbName).build())
  }

   def stopEmbeddedDb(db: EmbeddedMysql) = db.stop()

  def importData(port: Int, user: String, password: String, dataScript: String) = {
    val sql = if (dataScript.endsWith("zip")) FilesOps.unzipToTmp(dataScript) else List(dataScript)
    val connection: Connection = connectionString("localhost", port, user, password)
    try {
      val runner = new ScriptRunner(connection)
      sql.foreach(script => runner.executeScript(new File(script)))
    } finally {
      if (connection != null) connection.close()
    }

  }

  def connectionString(host: String, port: Int, user: String, password: String): Connection = {
    DriverManager.getConnection(s"jdbc:mysql://$host:$port", user, password)
  }
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
