/*
 *
 *  *  Licensed to the Apache Software Foundation (ASF) under one or more
 *  *  contributor license agreements.  See the NOTICE file distributed with
 *  *  this work for additional information regarding copyright ownership.
 *  *  The ASF licenses this file to You under the Apache License, Version 2.0
 *  *  (the "License"); you may not use this file except in compliance with
 *  *  the License.  You may obtain a copy of the License at
 *  *
 *  *     http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  *  Unless required by applicable law or agreed to in writing, software
 *  *  distributed under the License is distributed on an "AS IS" BASIS,
 *  *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  *  See the License for the specific language governing permissions and
 *  *  limitations under the License.
 *
 */

package org.dbtofile.conf

import org.dbtofile.DdToFileSuite

class DbTest extends DdToFileSuite {
  private val a2b = Relation("a", "aa", "b", "bb")
  private val a2c = Relation("a", "aa", "c", "cc")
  private val b2c: Relation = Relation("b", "bb", "c", "cc")
  private val b2z: Relation = Relation("b", "bb", "z", "zz")
  private val c2y: Relation = Relation("c", "cc", "y", "yy")
  private val c2d: Relation = Relation("c", "cc", "d", "dd")
  private val d2a: Relation = Relation("d", "dd", "a", "aa")
  private val d2k: Relation = Relation("d", "dd", "k", "kk")
  val relations: List[Relation] = List(
    a2b, a2c, b2c, b2z, c2y, c2d, d2a, d2k
  )

  test("find relations") {
    val db = Db("test-db", Array.empty[DbTable], relations.toArray)

    val aToBPath = db.findRelations("a", "b")
    val aToZPath = db.findRelations("a", "z")
    val aToKPath = db.findRelations("a", "k")
    val aToXPath = db.findRelations("a", "x")

    require(aToBPath == List(a2b), "Invalid a->b path")
    require(aToZPath == List(a2b, b2z), "Invalid a->z path")
    require(aToKPath == List(a2c, c2d, d2k), s"Invalid a->k path ${aToKPath}")
    require(aToXPath == Nil, "Invalid a->k path")
  }

}
