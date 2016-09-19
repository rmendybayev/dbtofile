package org.dbtofile

import org.dbtofile.helpers.MysqlOps
import org.scalatest.{BeforeAndAfterAll, FunSuite, Matchers}


trait DdToFileSuite extends FunSuite with Matchers with BeforeAndAfterAll {
  val dbPort: Int = 32769
  val dbUser: String = "db_user"
  val dbPass: String = "pass"
  val employeesDb: String = "employees"


  def withMySql(port: Int, user: String, password: String, initScript: String, dbName: String)(test: => Unit) = {
    val db = MysqlOps.embeddedDb(port, user, password)
    MysqlOps.createDatabase(db, dbName)
    MysqlOps.importData(dbPort, dbUser, dbPass, initScript)
    try {
      test
    } finally {
      if (db != null) {
        println("Embedded database server will be stopped")
        db.stop()
      }
    }

  }

  def withEmployeesDb(port: Int = dbPort, user: String = dbUser, password: String = dbPass) =
    withMySql(
      port = port,
      user = user,
      password = password,
      initScript = getClass.getResource("/employees_db.sql.zip").getFile,
      dbName = employeesDb) _
}

