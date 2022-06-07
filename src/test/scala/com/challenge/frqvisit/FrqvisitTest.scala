package com.challenge.frqvisit

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.BeforeAndAfterAll
import org.apache.spark.sql.SparkSession
import com.challenge.frqvisit.FrqVisitor._
import org.apache.spark.sql.Dataset
import com.challenge.frqvisit._

/**
 * This test class is to test FrqVisitor class.
 *
 * I created a sample test file "access_log_Aug95_temp" to run most of the test cases. The number of records in the
 * test file is less than original data file. There is no change in format of the data.
 * access_log_Aug95_temp test file is also uploaded in Github.
 *
 */
class FrqvisitTest extends AnyFunSuite with BeforeAndAfterAll {

  //Store SparkSession
  var spark: SparkSession = _;

  //Arguments which will be passed from command prompt.
  var argsL = Array("local", "5");

  /**
   * This is to store the raw data one time from my local test file (access_log_Aug95_temp).
   */
  var dfRawData: Dataset[AccessLogRec] = _;

  /**
   * This method will be called before all test method.
   *
   */
  override def beforeAll() {

    //Create spark session
    spark = SparkSession.builder().appName("FrequentVistors").master("local[2]").getOrCreate()

  }

  /**
   * Validate number of records in data file. There are 274835 records in my test file.
   *
   */
  test("Correct number of raw data fetched from local") {
    //Read raw data from test data file.
    dfRawData = readDatafileFromLocal(argsL, spark);
    assert(dfRawData.count() == 274835)
  }

  /**
   * Validate final row count. This should be 35. This is from my small test file.
   *
   */
  test("cleanAndFormatData method is returning correct number of record based on local test file") {

    val finalDF = cleanAndFormatData(spark, dfRawData, argsL(1).toInt);

    println("Count : " + finalDF.count());

    assert(finalDF.count() == 35)
  }

  /**
   * Validate number of columns of final DF. It should be 4.
   *
   */
  test("cleanAndFormatData method is returning correct number of columns") {

    val finalDF = cleanAndFormatData(spark, dfRawData, argsL(1).toInt);

    assert(finalDF.columns.length == 4)
  }

  /**
   * Successfully read all data from FTP zip.
   *
   */
  test("Successfull read data from FTP") {
    val rdd = readZipFileFromFtp(spark);

    //check the count of retrieve records
    assert(rdd.count() == 1891715)
  }

  /**
   * Successfully read all data from Github zip.
   *
   */
  test("Successfull read data from GitHub") {
    val rdd = readFileFromGit(spark);

    //Check the count of retrieve records.
    assert(rdd.count() == 1891715)
  }

  /**
   * This method will be called after completion of all test cases.
   */
  override def afterAll() {
    spark.close();
  }

}