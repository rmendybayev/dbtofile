package org.dbtofile.generator

import java.io.FileInputStream

import org.dbtofile.conf.Configuration
import org.scalatest.{FunSpec, Matchers}
import org.yaml.snakeyaml.Yaml
import org.yaml.snakeyaml.constructor.Constructor

import scala.concurrent.duration.Duration

class TableGeneratorSpec extends FunSpec with Matchers {

  val filename = "foo.yaml"

  it("should read yaml and properties and generate new file") {
    val input = new FileInputStream("src/test/resources/fhopehs.yaml")
    val yaml = new Yaml(new Constructor(classOf[org.dbtofile.conf.TableList]))
    val t = yaml.load(input).asInstanceOf[org.dbtofile.conf.TableList]
    val dateList = List("2018-08-09", "2018-08-10", "2018-08-11", "2018-08-12")

    val resultList = Configuration.generateConfigurationForDate(t, dateList, Duration("1 day"))
    Configuration.generateYamlConf(resultList, filename)
  }

  it("should print 4 tables") {
    val input = new FileInputStream(filename)
    val yaml = new Yaml(new Constructor(classOf[org.dbtofile.conf.TableList]))
    val t = yaml.load(input).asInstanceOf[org.dbtofile.conf.TableList]
    t.tables should have size 4
  }
}
