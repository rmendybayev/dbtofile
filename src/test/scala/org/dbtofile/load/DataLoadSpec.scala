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
    fileSystem.delete(new Path("output/dbtofile"), true)
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
      //assert(output.schema === input.schema)
    }
  }


  it("should print all type of schemas") {
//    spark.read.parquet("output/dbtofile-without").printSchema()
//    spark.read.parquet("output/dbtofile").filter("OPE_NO = '2570.1700'").show(false)
//    spark.read.parquet("output/ongoing/siview_mmdb.fhopehs").printSchema()
//    spark.read.parquet("input/ongoing").printSchema()
    spark.read.parquet("input/ongoing").filter("OPE_NO = '2570.1700'").show(false)
  }

  it("should read output from batch-ingestion and compare") {
    val dbtofile = "output/db/SIVIEW_MMDB.FHOPEHS"
    val ongoing = "output/ongoing/SIVIEW_MMDB.FHOPEHS"

    val df1 = spark.read.parquet(dbtofile)
    val df2 = spark.read.parquet(ongoing)

    assert(df1.count() === df2.count())
    val df11 = df1.filter("OPE_NO = '2570.1700'")
    val df12 = df2.filter("OPE_NO = '2570.1700'")
    df11.show(false)
    df12.show(false)
    df11.union(df12).distinct().except(df11.intersect(df12)).show(false)
    assert(df11.intersect(df12).count() > 0)
    assert(df11.union(df12).distinct().except(df11.intersect(df12)).count() === 0)
  }
}
