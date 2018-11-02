package org.dbtofile.schema

import com.databricks.spark.avro.SchemaConverters
import org.apache.avro.Schema
import org.apache.spark.sql.functions.{current_timestamp, lit, monotonically_increasing_id}
import org.apache.spark.sql.{DataFrame, Dataset, Row}
import org.apache.spark.sql.types.{DataType, StructType}

class SchemaConverter(schemaRegistry: SchemaRegistry) {
  def convertToSqlSchema(schema: Schema): StructType = {
    SchemaConverters.toSqlType(schema).dataType match {
      case t: StructType => Some(t).get
      case _ => throw new RuntimeException(
        s"""Avro schema cannot be converted to a Spark SQL StructType:
           |
           |${schema.toString(true)}
           |""".stripMargin)
    }
  }

  def transformTable(tableDS: Dataset[Row], schemaName: String, tableName: String): Dataset[Row] = {
    val schema = schemaRegistry.recordSchema(schemaName)
    val sqlSchema = convertToSqlSchema(schema)

    import org.apache.spark.sql.functions._
    val columns = tableDS.schema.toList.diff(sqlSchema.toList).map(_.name)

    val bulk = bulkCastToSchema(tableDS, columns, sqlSchema)
    val cols = addGGColumns(bulk.columns)

    val output = withGGColumns(bulk)
      .withColumn("table", lit(tableName).cast("string"))
      .select("table", cols:_*)
    output
  }


  private def bulkCastToSchema(df: DataFrame, columns: List[String], schema: StructType): DataFrame = {
    def cast(df: DataFrame, columns: List[String], schema: StructType): DataFrame = {
      if (columns.nonEmpty) {
        val column = columns.head
        cast(castColumnTo(df, column, schema.apply(column).dataType), columns.tail, schema)
      } else {
        df
      }
    }
    cast(df, columns, schema)
  }

  private def castColumnTo(df: DataFrame, cn: String, tpe: DataType) : DataFrame = {
    df.withColumn(cn, df(cn).cast(tpe))
  }

  private def addGGColumns(cols: Seq[String]): Seq[String] = "op_type" +: "op_ts" +: "current_ts" +: "pos" +:cols

  private def withGGColumns(df: DataFrame): DataFrame = {
    df.withColumn("op_type", lit("I").cast("string"))
      .withColumn("op_ts", lit(current_timestamp()).cast("string"))
      .withColumn("current_ts", lit(current_timestamp()).cast("string"))
      .withColumn("pos", lit(monotonically_increasing_id()).cast("string"))
  }

}

object SchemaConverter {
  def apply(schemaRegistryUrl: String): SchemaConverter = {
    val schemaRegistry = SchemaRegistry.apply(schemaRegistryUrl)
    new SchemaConverter(schemaRegistry)
  }
}
