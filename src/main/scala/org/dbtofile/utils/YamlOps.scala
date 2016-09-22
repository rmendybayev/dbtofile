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

package org.dbtofile.utils


import java.io.{BufferedWriter, File, FileWriter}

import org.yaml.snakeyaml.DumperOptions.FlowStyle
import org.yaml.snakeyaml.{DumperOptions, Yaml}
import org.yaml.snakeyaml.introspector.BeanAccess
import org.yaml.snakeyaml.nodes.Tag
import org.yaml.snakeyaml.representer.Representer

object YamlOps {

  def generateYaml[T](entity: T): Yaml = {
    val options = new DumperOptions
    options.setDefaultFlowStyle(FlowStyle.BLOCK)
    options.setExplicitStart(false)

    val representer = new Representer()
    representer.addClassTag(entity.getClass, Tag.MAP)

    val yaml = new Yaml(representer, options)

    yaml.setBeanAccess(BeanAccess.FIELD)
    yaml
  }

  def toString [T] (entity: T) = {
    val yaml: Yaml = generateYaml(entity)
    yaml.dump(entity)
  }

  def toString [T](entity: T, outputFile: File) = {
    val bw = new BufferedWriter(new FileWriter(outputFile))
    val yaml: Yaml = generateYaml(entity)
    yaml.dump(entity, bw)
    bw.close()
  }
}
