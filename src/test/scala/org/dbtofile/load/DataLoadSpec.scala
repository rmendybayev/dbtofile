package org.dbtofile.load

import java.io.FileInputStream

import com.typesafe.config.ConfigFactory
import org.dbtofile.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark
import org.dbtofile.SparkSessionTestWrapper
import org.scalatest.{BeforeAndAfter, FunSpec, Matchers}
import org.yaml.snakeyaml.Yaml
import org.yaml.snakeyaml.constructor.Constructor

import scala.concurrent.duration.Duration

class DataLoadSpec extends FunSpec
  with Matchers
  with SparkSessionTestWrapper
  with BeforeAndAfter {

  before {
    val fileSystem = FileSystem.get(new java.net.URI("output"), new org.apache.hadoop.conf.Configuration())
    fileSystem.delete(new Path("output/dbtofile"), true)
  }

  it("should read yaml to load table") {
    val appConf = ConfigFactory.load("test_app.conf")
    val input = new FileInputStream(appConf.getString("conf"))
    val yaml = new Yaml(new Constructor(classOf[org.dbtofile.conf.TableList]))
    val t = yaml.load(input).asInstanceOf[org.dbtofile.conf.TableList]

    t.tables should have size 1

    import collection.JavaConversions._
    val tableLIst =  if (appConf.getBoolean("generate.enabled"))
      Configuration.generateConfigurationForDate(t, appConf.getStringList("generate.datelist").toList, Duration(appConf.getString("generate.duration"))) else t

    tableLIst.tables should have size 4

    for (elem <- tableLIst.tables) {
      DataLoader.loadData(elem, spark, appConf)
    }

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
