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

package org.dbtofile.helpers

import java.io.{FileInputStream, FileOutputStream}
import java.nio.file.Files
import java.util.zip.ZipInputStream


object FilesOps {
  lazy val TMPDIR = Files.createTempDirectory("tmp")

  def unzipToTmp(archive: String): List[String] = unzip(archive, s"${TMPDIR.toAbsolutePath}/")

  def unzip(archive: String, dstDir: String): List[String] = {
    val zip = new ZipInputStream(new FileInputStream(archive))
    val files = Stream.continually(zip.getNextEntry)
      .takeWhile(_ != null)
      .map(entry => {
        val outpath = s"${dstDir.stripSuffix("/")}/${entry.getName}"
        println(s"Extracting entry from $archive to $outpath")

        val out = new FileOutputStream(outpath)
        try {
          val buffer = new Array[Byte](1024)
          Stream.continually(zip.read(buffer))
            .takeWhile(_ != -1)
            .foreach(x => {
              out.write(buffer, 0, x)
            })
        } finally {
          if (out != null) out.close()
        }
        outpath
      })
    files.toList
  }

}
