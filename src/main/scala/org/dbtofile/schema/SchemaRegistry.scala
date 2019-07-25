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
package org.dbtofile.schema

import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient
import org.apache.avro.Schema
import org.apache.avro.Schema.Type

import scala.collection.JavaConversions._
import scala.language.implicitConversions

class SchemaRegistry(val baseUrl: String) extends Serializable {

  private[this] val registry = new CachedSchemaRegistryClient(baseUrl, 100)

  def listNames: Array[String] = {
    //convert json string to scala array
    scala.io.Source.fromURL(s"$baseUrl/subjects").mkString
      .stripPrefix("[").stripSuffix("]")
      .replace("\"", "").split(",")
  }

  def schema(name: String) = {
    new Schema.Parser().parse(registry.getLatestSchemaMetadata(name).getSchema)
  }


  def keySchema(table: String) = schema(s"$table-key")

  def recordSchema(table: String): Schema = schema(s"$table-value")

  def keyColumns(table: String): Array[String] = {
    val avroSchema: Schema = schema(s"$table-key")
    avroSchema.getFields.map(_.name()).toArray
  }

  def columns(table: String): Array[String] = {
    val avroSchema = schema(s"$table-value")
    avroSchema.getField("before").schema().getTypes
      .filter(_.getType != Type.NULL)
      .flatMap(_.getFields.map(_.name())).toArray
  }

}

object SchemaRegistry {
  def apply(baseUrl: String): SchemaRegistry = new SchemaRegistry(baseUrl)

}
