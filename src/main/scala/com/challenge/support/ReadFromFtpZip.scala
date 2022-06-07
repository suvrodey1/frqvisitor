package com.challenge.support

import java.io.FileInputStream
import java.util.zip.ZipInputStream
import java.io.BufferedInputStream
import scala.io.Source
import java.util.zip.GZIPInputStream
import java.net.URL
import scala.io._
import java.io.InputStream
import scala.collection.mutable.ArrayBuffer

/**
 * This is the class is used to read the data file (zip) from ftp location.
 *
 * The ftp filename and location : ftp://ita.ee.lbl.gov/traces/NASA_access_log_Jul95.gz
 *
 */
class ReadFromFtpZip extends ReadDataFileT {

  //ftp location. Read the zip file from this location.
  val zipFilename = "ftp://ita.ee.lbl.gov/traces/NASA_access_log_Jul95.gz"

  /**
   * This is to read the zip data file from ftp location and return array object.
   *
   * @return Array of string where each item of array contain each line of file.
   */
  def readDataFile(): Array[String] = {

    var is: InputStream = null

    try {

      //Input tream to read zip file.
      is = new URL(zipFilename).openStream();

      //Store all lines in Iterator[String] object
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

      println("Lines in FTP Zip file : " + arrBufLines.length)

      if (arrBufLines.toArray.length > 0) {
        //return Array[String] object
        arrBufLines.toArray
      } else {

        //Throw exception incase unable to read the file.
        throw new Exception("Unable to read data file from ftp!!!!");
      }
    } finally {
      //Close stream.
      is.close()
    }
  }

}