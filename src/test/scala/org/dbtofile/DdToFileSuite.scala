package org.dbtofile

import org.dbtofile.helpers.{EmbeddedH2, EmbeddedMySql}
import org.scalatest.{BeforeAndAfterAll, FunSuite, Matchers}


trait DdToFileSuite extends FunSuite with Matchers with BeforeAndAfterAll {
  val dbPort: Int = 32769
  val dbUser: String = "db_user"
  val dbPass: String = "pass"
  val employeesDb: String = "employees"

  def  db = new EmbeddedH2

  def withEmbeddedDb(port: Int, user: String, password: String, initScript: String, dbName: String)(test: => Unit) = {
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

  def withEmployeesDb(port: Int = dbPort, user: String = dbUser, password: String = dbPass) =
    withEmbeddedDb(
      port = port,
      user = user,
      password = password,
      initScript = getClass.getResource("/employees_db.sql").getFile,
      dbName = employeesDb) _
}

