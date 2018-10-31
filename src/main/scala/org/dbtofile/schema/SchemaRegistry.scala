package org.dbtofile.schema

import com.databricks.spark.avro.SchemaConverters
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient
import org.apache.avro.Schema
import org.apache.avro.Schema.Type
import org.apache.spark.sql.{DataFrame, Dataset, Row}
import org.apache.spark.sql.types.{DataType, StructType}

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
