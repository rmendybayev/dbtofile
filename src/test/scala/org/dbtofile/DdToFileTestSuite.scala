package org.dbtofile

import org.dbtofile.helpers.EmbeddedDbOps


trait DdToFileTestSuite {

  def withMySql(port: Int, user: String, password: String, initScript: String, dbName:String = "employees")(test: => Unit) = {
    val db = EmbeddedDbOps.startDb(port, user, password, dbName)
    val schema = EmbeddedDbOps.importData(db, initScript)
    try test finally if (db != null) db.stop()

  }
}
