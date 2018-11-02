package org.dbtofile.load

import java.io.FileInputStream

import com.typesafe.config.ConfigFactory
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.dbtofile.SparkSessionTestWrapper
import org.scalatest.{BeforeAndAfter, FunSpec}
import org.yaml.snakeyaml.Yaml
import org.yaml.snakeyaml.constructor.Constructor

class DataLoadSpec extends FunSpec
  with SparkSessionTestWrapper
  with BeforeAndAfter {

  before {
    val fileSystem = FileSystem.get(new java.net.URI("output"), new Configuration())
    //fileSystem.delete(new Path("output/FHOPEHS.parquet"), true)
  }

  it("should read yaml to load table") {
    val appConf = ConfigFactory.load("test_app.conf")
    val input = new FileInputStream(appConf.getString("conf"))
    val yaml = new Yaml(new Constructor(classOf[org.dbtofile.conf.TableList]))
    val t = yaml.load(input).asInstanceOf[org.dbtofile.conf.TableList]

    for (table <- t.tables) {
      DataLoader.loadData(table, spark, appConf)
      val output = spark.read.parquet(table.outputPath)
      val input = spark.read.parquet("input/FHOPEHS.parquet")
      assert(output.schema === input.schema)
    }
  }

  it("should validate output parquet") {
    val output = spark.read.parquet("output/FHOPEHS.parquet")
    output.select("table", "op_type", "op_ts", "current_ts", "pos").show(5, false)
    assert(output.select("pos").distinct().count() === output.count())

  }

  it("should show current_date") {
    import org.apache.spark.sql.functions._
    val input = spark.read.parquet("input/FHOPEHS.parquet").withColumn("op_ts_1", lit(current_timestamp()))
    input.select("table", "op_ts", "op_ts_1").show(5, false)
  }
}
