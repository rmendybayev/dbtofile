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

package org.dbtofile.conf

import org.dbtofile.DdToFileSuite
import org.scalatest.Tag

class ListItemOps[T](item: T) {
  def in(list: Seq[T]): Boolean = list.contains(item)
}

object ListItemOps {
  implicit def stringToListItem(item: String):ListItemOps[String] = new ListItemOps(item)
}

class DataSourceDefinitionTest extends DdToFileSuite {
  test("load data source definitions from table metadata", Tag("slow")) {
    withEmployeesDb() {
      val ds: Db = DataSourceDefinition.load(db.connectionString(Some("employees")), "sa", "")
      require(ds.tables.length == 37) //H2 creates a lot of service tables
      val childList = ds.childTables("employees").map(_.name)
      require(childList.size == 4)
      import org.dbtofile.conf.ListItemOps._
      require("dept_emp" in childList, s"dept_emp is missing : ${childList.mkString(";")}")
      require("titles" in childList,  s"departments is missing : ${childList.mkString(";")}")
      require("dept_manager" in childList, s"dept_manager is missing : ${childList.mkString(";")}")
      require("salaries" in childList, s"salaries is missing : ${childList.mkString(";")}")


    }
  }

}
