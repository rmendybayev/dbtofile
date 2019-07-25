package org.dbtofile.reader

import java.io.FileInputStream

import org.dbtofile.SparkSessionTestWrapper
import org.dbtofile.schema.{SchemaConverter, SchemaRegistry}
import org.scalatest.{BeforeAndAfter, FunSpec}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.types._
import org.yaml.snakeyaml.Yaml
import org.yaml.snakeyaml.constructor.Constructor


class DataReaderSpec
  extends FunSpec with BeforeAndAfter
  with SparkSessionTestWrapper {
  var converter: SchemaConverter = null

  before {
    val fileSystem = FileSystem.get(new java.net.URI("output"), new Configuration())
    //fileSystem.delete(new Path("output/FRLOT.avro"), true)
  }

  it("should convert avro schema to sql structure") {
    val schemaRegistry = new SchemaRegistry("http://localhost:8888")
    val schema = schemaRegistry.recordSchema("gf-f8-bigdata-clouddev-dbgemd-data-FHOPEHS")
    converter = SchemaConverter.apply("http://localhost:8888")

    val outPutSchemaStructType: StructType = converter.convertToSqlSchema(schema)
    println(outPutSchemaStructType)
  }

  it("should read parquet") {
    val dataFrame = spark.read.parquet("input/FRLOT.parquet")
    dataFrame.printSchema()
    assert(dataFrame.count() === 999)
    dataFrame.show(1)
  }

  it("should read parquest, apply AVRO schema and extract parquet") {
    val schemaRegistry = new SchemaRegistry("http://10.210.33.10:8081")
    val schema = schemaRegistry.recordSchema("gf-f8-bigdata-clouddev-dbgemd-data-FRLOT")
    val sqlSchema = converter.convertToSqlSchema(schema)
    val inputPqt = spark.read.parquet("input/FRLOT.parquet")

    val outputPqt = spark.createDataset(inputPqt.rdd)(RowEncoder(sqlSchema))
    assert(outputPqt.count() === 999)
    outputPqt.show(1)

  }

  it("should auto cast field to string. Example on CREATED_TIME") {
    val schemaRegistry = new SchemaRegistry("http://10.210.33.10:8081")
    val schema = schemaRegistry.recordSchema("gf-f8-bigdata-clouddev-dbgemd-data-FRLOT")
    val sqlSchema = converter.convertToSqlSchema(schema)


    val inputPqt = spark.read.parquet("input/FRLOT.parquet")

    val columns = inputPqt.schema.toList.diff(sqlSchema.toList).map(_.name)
    import org.apache.spark.sql.functions._
    val bulk = bulkCast(inputPqt, columns, sqlSchema)

    val cols = "op_type" +: "op_ts" +: "current_ts" +: "pos" +:bulk.columns

    val outputPqt = bulk
      .withColumn("table", lit(null).cast("string"))
      .withColumn("op_type", lit(null).cast("string"))
      .withColumn("op_ts", lit(null).cast("string"))
      .withColumn("current_ts", lit(null).cast("string"))
      .withColumn("pos", lit(null).cast("string"))
      .select("table", cols:_*)
    println(sqlSchema.toList.diff(outputPqt.schema.toList))
    assert(outputPqt.schema === sqlSchema)

  }

  it("should parge yaml conf file") {
    val input = new FileInputStream("src/main/resources/tables_conf.yaml")
    val yaml = new Yaml(new Constructor(classOf[org.dbtofile.conf.TableList]))
    val t = yaml.load(input).asInstanceOf[org.dbtofile.conf.TableList]
    for (table <- t.tables) {
      println("Hello")
      //DataLoader.loadData(table, spark)
    }
  }

  it("should convert double with higher precision") {
    import org.apache.spark.sql.functions._
    val value = "769.227662037037020"
    val testDf = spark.read.parquet("input/FHOPEHS.parquet").withColumn("test", lit(value))
    assert(castColumnTo(testDf, "test", DoubleType).select("test").first().get(0).toString === value)
    assert(castColumnTo(testDf, "test", DecimalType(30, 15)).select("test").first().get(0).toString === value)
  }



  def castColumnTo(df: DataFrame, cn: String, tpe: DataType) : DataFrame = {
    val trasformType = tpe match {
      case t:DoubleType => DecimalType(30, 15)
      case f:DataType => f
    }
    df.withColumn(cn, df(cn).cast(trasformType))
  }

  def bulkCast(df: DataFrame, columns: List[String], schema: StructType): DataFrame = {
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

}
