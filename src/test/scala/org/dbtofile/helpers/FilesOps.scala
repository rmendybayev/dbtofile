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
