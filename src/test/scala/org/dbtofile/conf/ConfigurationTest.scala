package org.dbtofile.conf

import java.io.File

import org.dbtofile.DdToFileSuite

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

}
