package pds

import java.io._
import java.nio.file.{Files, Paths}


/**
 * Utility object for File access.
 */
object FileUtils {

  type FileName = String
  type Dir = String

  def withFile[A](fileName: FileName)(func: PrintWriter => A): Unit = {
    val file = new File(fileName)
    val write = new PrintWriter(file)
    try {
      func(write)
    } finally {
      write.close()
    }
  }

  def fromFile(filePath: FileName, encoding: String = "iso-8859-1"): Iterator[String] = scala.io.Source.fromFile(filePath, encoding).getLines

  def readFile(filePath: FileName, encoding: String = "iso-8859-1"): String = fromFile(filePath, encoding).mkString("\n")

  def readBinaryFile(fileName: FileName): Array[Byte] = {
    Files.readAllBytes(Paths.get(fileName))
  }

  // scalastyle:off regex
  def write(fileName: FileName, iterator: Iterator[String]): Unit = {
    withFile(fileName) { output =>
      iterator.foreach(line => output.println(line))
    }
  }

  // scalastyle:on regex

  def write(fileName: FileName, value: String): Unit = {
    write(fileName, Iterator.single(value))
  }

  def write(fileName: FileName, array: Array[Byte]): Unit = {
    import java.io.FileOutputStream
    val fos = new FileOutputStream(fileName)
    fos.write(array)
    fos.close()
  }

  def write(fileName: FileName, stream: InputStream): Unit = {
    Files.copy(stream, new java.io.File(fileName).toPath)
  }

  def copyFile(srcPath: String, destPath: String): Unit = {
    val src = new File(srcPath)
    val dest = new File(destPath)
    new FileOutputStream(dest).getChannel.transferFrom(
      new FileInputStream(src).getChannel, 0, Long.MaxValue)
  }

  def exist(path: String): Boolean = {
    new java.io.File(path).exists
  }

  def delete(fileName: FileName): Boolean = {
    new File(fileName).delete()
  }

  def fileSize(fileName: FileName): Long = {
    new File(fileName).length()
  }

  def appendLine(fileName: FileName, value: String): Unit = {
    val fileWriter = new FileWriter(fileName, true)
    try {
      fileWriter.write(value)
      fileWriter.write("\n")
    } finally {
      fileWriter.close()
    }
  }
}
