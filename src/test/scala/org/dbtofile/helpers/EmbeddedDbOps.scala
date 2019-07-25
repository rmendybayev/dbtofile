package org.dbtofile.helpers

import java.io.File
import java.sql.{Connection, DriverManager}
import java.util.Scanner

import com.wix.mysql.EmbeddedMysql
import com.wix.mysql.EmbeddedMysql.Builder
import com.wix.mysql.config.{Charset, MysqldConfig}
import com.wix.mysql.distribution.Version

import scala.util.{Failure, Try}

object EmbeddedDbOps {
  def startDb(port: Int,
              userName: String,
              password: String,
              dbName: String
             ): EmbeddedMysql = {
    val conf = MysqldConfig.aMysqldConfig(Version.v5_7_latest)
      .withCharset(Charset.UTF8)
      .withPort(port)
      .withUser(userName, password).
      build()
    EmbeddedMysql.anEmbeddedMysql(conf).addSchema(dbName).start()
  }

  def stopDb(db: EmbeddedMysql) = db.stop()

  def importData(db: EmbeddedMysql, dataScript: String) = {
    val sql = if (dataScript.endsWith("zip")) FilesOps.unzipToTmp(dataScript) else List(dataScript)
    val conf = db.getConfig
    val connection: Connection = DriverManager.getConnection(s"jdbc:mysql://localhost:${conf.getPort}", conf.getUsername, conf.getPassword)
    try {
      val runner = new ScriptRunner(connection)
      sql.foreach(script => runner.executeScript(new File(script)))
    } finally {
      if (connection != null) connection.close()
    }

  }

}
