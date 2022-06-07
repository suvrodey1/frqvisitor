package com.challenge.support

import scala.io.Source
import scala.collection.mutable.ArrayBuffer

import java.io.FileInputStream
import java.util.zip.ZipInputStream
import java.io.BufferedInputStream
import scala.io.Source
import java.util.zip.GZIPInputStream
import java.net.URL
import scala.io._
import java.io.InputStream
import org.apache.spark.sql.SparkSession
import java.net.URLEncoder
import java.nio.charset.StandardCharsets

/**
 * Read the data file from GitHub location.
 * Github file location - https://github.com/suvrodey1/frqvisitor/blob/main/NASA_access_log_Jul95
 *
 */
class ReadFromGit extends ReadDataFileT {

  val githubZipFilename = "https://github.com/suvrodey1/frqvisitor/blob/main/NASA_access_log_Jul95.gz?raw=true"

  /**
   * This is to read the zip data file from ftp location and return array object.
   *
   * @return Array of string where each item of array contain each line of file.
   */
  def readDataFile(): Array[String] = {

    var is: InputStream = null;

    try {

      val is = new URL(githubZipFilename).openStream();
      val lines: Iterator[String] =
        Source
          .fromInputStream(new GZIPInputStream(is))(Codec.ISO8859)
          .getLines

      //Store all lines in ArrayBuffer
      val arrBufLines = new ArrayBuffer[String]()

      lines.foreach(line =>
        {
          //println(a)
          arrBufLines += line
        })

      println("Lines in Git Zip file : " + arrBufLines.length)

      if (arrBufLines.toArray.length > 0) {
        //return Array[String] object
        arrBufLines.toArray
      } else {

        //Throw exception incase unable to read the file.
        throw new Exception("Unable to read data file from ftp!!!!");
      }
    } catch {
      case e: Throwable => {
        println("Unable to read file from Git : " + e.getMessage());

        throw e;
      }
    } finally {
      //Close stream.
      if (is != null) is.close()
    }
  }

}