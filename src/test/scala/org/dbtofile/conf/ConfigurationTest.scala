package org.dbtofile.conf

import java.io.File

import org.dbtofile.DdToFileSuite
import org.scalatest.Tag

import scala.collection.mutable.ArrayBuffer

class ConfigurationTest extends DdToFileSuite {

  test("load configurations from YAML") {
    val yaml = getClass getResource "/tables_conf.yaml"
    val conf = Configuration.loadConfigurationFromYaml(new File(yaml.getFile))
    conf.tables.length should be(7)
    conf.merges.length should be(1)

    val tables = conf.tables.groupBy(_.table)
    val employees = tables("employees").headOption
    val departments = tables("departments").headOption
    require(employees.isDefined)
    require(departments.isDefined)

    employees.map(_.password) should be(departments.map(_.password))
    employees.map(_.url) should be(departments.map(_.url))
    employees.map(_.user) should be(departments.map(_.user))
    employees.map(_.outputPath).get should be("target/employee.parquet")
    departments.map(_.outputPath).get should be("target/departments.parquet")

    val merge = conf.merges.head
    merge.outputTable.outputPath should be("target/merged_employee.parquet")
  }

  ignore("load configuration from table metadata", Tag("slow")) {
    val connectionString = db.connectionString(Some("employees"))
    val user = "sa"
    val password = ""
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

        val tableInfo: List[MergeInfo] = tables.flatMap(table => {
          println(s"TABLE : $table")
          val tInfo=TableInfo(
            url=db.connectionString(Option(table)),
            table = table,
            password=password,
            user = user,
            outputPath = null,
            outputFormat = null,
            sql = null,
            load = false)
          val foreignKeys = metadata.getImportedKeys(catalog, null, table)
          val mergeOps = ArrayBuffer[(MergeTableInfo,MergeTableInfo)]()
          while (foreignKeys.next()) {
            val base = MergeTableInfo(tInfo, foreignKeys.getString("PKCOLUMN_NAME"))
            val childTInfo=TableInfo(
              url=db.connectionString(Option( foreignKeys.getString("FKTABLE_NAME"))),
              table =  foreignKeys.getString("FKTABLE_NAME"),
              password=password,
              user = user,
              outputPath = null,
              outputFormat = null,
              sql = null,
              load = false)

            val child = MergeTableInfo(childTInfo, foreignKeys.getString("FKCOLUMN_NAME"))

            mergeOps += ((base, child))
          }
          mergeOps.groupBy(_._1).map{case (baseTable, tablesToJoin)=> MergeInfo(baseTable, null, tablesToJoin.map(_._2).toArray)}
        })


      } finally {
        if (conn != null) conn.close()
      }

    }
  }

}
