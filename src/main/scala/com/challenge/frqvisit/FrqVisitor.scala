package com.challenge.frqvisit

import scala.collection.generic.Sorted
import scala.collection.SortedMap
import scala.util.Random
import scala.collection.mutable
import scala.collection.mutable.HashMap
import java.util.Scanner
import scala.collection.mutable.Set
import scala.collection.mutable.ListBuffer
import scala.collection.mutable.ArrayBuffer
import org.apache.spark.sql.catalyst.expressions.aggregate.Last
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions._
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.DataFrame
import org.apache.log4j.Logger
import org.apache.spark.sql.SaveMode
import java.sql.Date
import com.challenge.support.ReadFromGit
import org.apache.spark.rdd.RDD
import com.challenge.support.ReadFromFtpZip
import org.apache.spark.sql.Dataset

/**
 * This Spark job is developed to fulfill following requirements -
 * ---------------------------------------------------------
 * 1) Download the zip file from ftp location (ftp://ita.ee.lbl.gov/traces/NASA_access_log_Jul95.gz).
 * 2) If it failed to access the ftp location then pull data from GitHub location (https://raw.githubusercontent.com/suvrodey1/frqvisitor/main/access_log_Aug95)
 * 3) Process the data to find the top-n most frequent visitors and urls for each day of the trace. The value of n will be passed as argument.
 *
 * Findings from the data file -
 * -----------------------------
 * 1) Name of Zip file - ftp://ita.ee.lbl.gov/traces/NASA_access_log_Jul95.gz
 * 2) Github location - https://raw.githubusercontent.com/suvrodey1/frqvisitor/main/access_log_Aug95
 *
 * 3) Example of data -
 * 	in24.inetnebr.com - - [01/Aug/1995:00:00:01 -0400] "GET /shuttle/missions/sts-68/news/sts-68-mcc-05.txt HTTP/1.0" 200 1839
 * 3) Data format
 * ( This can also be found in https://httpd.apache.org/docs/2.4/logs.html) -
 * 		a) visitor ID
 * 		b) -
 * 		c) -
 * 		d) [ access day in the format of dd/mmm/yyyy:mm:hh:ss -zone]
 * 		e) url called
 * 		f) Status from server
 * 		g) Size of the returned object
 *
 */

/**
 * This case class is used store each vistor records.
 */
case class AccessLogRec(hostname: String, accessTime: String, url: String)

/**
 * This case class is used store each vistor records and the date of access.
 */
case class AccessLogRecWithDate(hostname: String, url: String, accessDate: Date)

/**
 * This case class is used store each vistor records and total access count by hostname + url + accessdate.
 */
case class AccessLogRecWithDateCount(hostname: String, url: String, accessDate: Date, count: Integer)

object FrqVisitor {

  //Logger object
  lazy val LOG = Logger.getLogger(this.getClass)

  /**
   * Pattern used to parse each record of data file.
   */
  val PATTERN = """^(\S+) (\S+) (\S+) \[(\S+) (\S+) (\S+) (\S+)""".r

  /**
   * Main method.
   *
   *
   */
  def main(args: Array[String]) {

    //validate arguments
    if (!validateArgs(args: Array[String])) {
      //if aruments are not valid.
      return ;
    }

    /** Initialize SparkSession object. */
    val spark = getSparkSession(args(0));

    //Read the data file either from ftp location or Github
    val dsAccessLogRec = readDataFile(args, spark);

    //val dsAccessLogRec = readDatafileFromLocal(args, spark)

    //Cache the Dataframe records retrieve from the ftp/git.
    dsAccessLogRec.cache();
    dsAccessLogRec.collect()

    //This is to clean the data and return the list of top n record by accessDate.
    val dsTopNRec = cleanAndFormatData(spark, dsAccessLogRec, args(1).toInt);

    //Print or Save value.
    displayOrSaveOutput(args(0) == "local", dsTopNRec)

    //Close SparkSession
    spark.close()
  }

  /**
   * Validate the passed argument from command prompt.
   *
   * [1] : Environment type - local or prod
   * [2] : The value of n where n is the top count for each date.
   */
  def validateArgs(args: Array[String]): Boolean = {
    //Environment information not passed.
    if (args.length <= 0) {
      println("You must pass arguments : 1) local or prod. 2) Required number of top count (n). ");
      false

    } else if (args.length <= 1) {
      println("You must pass arguments : Required number of top count (n). ");
      false

    } else {
      true
    }
  }

  /**
   * Create SparkSession object.
   *
   *
   * @param args Argument passed from command prompt. Possible values as local and prod
   * 		"local" means, the job we are running in local environment
   * 		"prod" means, the job we are running in prod-cluster environment
   */
  def getSparkSession(environment: String): SparkSession = {

    //Spark configuration.
    if (environment == "local") {
      SparkSession.builder().appName("Code_Challenge").master("local[1]").getOrCreate();
    } else {
      //Configuration for cluster environment
      SparkSession.builder().appName("Code_Challenge").getOrCreate();
    }

  }

  /**
   * Read the raw data from ftp location and retun RDD objet.
   *
   * @param spark Spark Session object
   *
   * @return RDD object containing data.
   */
  def readZipFileFromFtp(spark: SparkSession): RDD[String] = {

    //Initialize the class
    val readFromFtpZip = new ReadFromFtpZip()

    //Read the data file. The method "readDataFile" return array of string where each item of array contain each line of file.
    val lineArr = readFromFtpZip.readDataFile()

    //Load the array object in RDD. Default partition has been used. In real environment and depend on size of file we should pass appropriate partition number.
    val rawRdd = spark.sparkContext.parallelize(lineArr);

    //return
    rawRdd
  }

  /**
   * Read the raw data from Github.
   *
   * @param spark Spark Session object
   *
   * @return RDD object containing data.
   */
  def readFileFromGit(spark: SparkSession): RDD[String] = {

    //Initialize the class
    val readFromGit = new ReadFromGit();

    //Read the data file. The method "readDataFile" return array of string where each item of array contain each line of file.
    val lineArr = readFromGit.readDataFile();

    //lineArr.foreach(println)

    //Load the array object in RDD. Default partition has been used. In real environment and depend on size of file we should pass appropriate partition number.
    val rawRdd = spark.sparkContext.parallelize(lineArr);

    //return
    rawRdd
  }

  /**
   * Read the data file from ftp site or GitHub and create DS.
   *
   * @param args Argument passed from command prompt.
   * @param spark SparkSession object
   *
   * @return Dataset having records of type AccessLogRec
   */
  def readDataFile(args: Array[String], spark: SparkSession): Dataset[AccessLogRec] = {

    //This is to store Raw data from file.
    var rawRdd: RDD[String] = null

    try {
      println("Reading from ftp.");

      //Read the raw data from ftp folder
      rawRdd = readZipFileFromFtp(spark);

    } catch {
      case err: Throwable => {
        //Show error message
        println("Unable to read zip from ftp. " + err.getMessage)

        println(err.printStackTrace())

        println("Reading from Git.");
        //Read the raw data from Git incase unable to read from ftp.
        rawRdd = readFileFromGit(spark);
      }
    }

    import spark.implicits._

    //Populate the Dataset where the records are of AccessLogRec type
    val dsAccessLogRec = rawRdd.map(convertLogRec).toDS()

    if (LOG.isDebugEnabled()) {
      dsAccessLogRec.show()
    }

    //Return the Dataset
    dsAccessLogRec;
  }

  /**
   * IGNORE THIS METHOD.
   * This method is used to test logic using local file.
   *
   * The format of the file is exact same but the record count is less.
   *
   */
  def readDatafileFromLocal(args: Array[String], spark: SparkSession): Dataset[AccessLogRec] = {
    val filename = "C:/Users/P2819547/Desktop/suvro-root/delete/challenge/NASA_access_log_Aug95/access_log_Aug95_temp"

    val rddLogs = spark.sparkContext.textFile(filename);
    import spark.implicits._
    val dsAccessLogRec = rddLogs.map(convertLogRec).toDS()
    dsAccessLogRec
  }

  /**
   * This is to clean the data and return the list of top n record by accessDate.
   * Step 1 - Sort the record by hostname + url + access date
   * Step 2 - Group the records by accessDate
   * Step 3 - get topVisitorCount (top n) from each group.
   *
   * @param spark SparkSession.
   * @param dsAccessLogRec Dataset containing AccessLogRec object.
   * @param topVisitorCount Collect these many top vistors for each day.
   *
   * @return DataSet containing top-n most frequent visitors(hostname) by each accessDate
   */
  def cleanAndFormatData(
    spark:           SparkSession,
    dsAccessLogRec:  Dataset[AccessLogRec],
    topVisitorCount: Integer): Dataset[AccessLogRecWithDateCount] = {

    if (LOG.isDebugEnabled()) {
      dsAccessLogRec.show();
    }

    import spark.implicits._

    /**
     * Convert the tiamestamp column to date column i.e accessTime to accessDate column.
     * Filterout if any of the hostname is blank.
     *
     * The columns of the dfLogWithDate will be - hostname: String, url: String, accessDate
     */
    import spark.implicits._
    val dsAccessLogRecWithDate = dsAccessLogRec.withColumn("accessDate", to_date(col("accessTime"), "dd/MMM/yyyy:HH:mm:ss"))
      .drop("accessTime").filter(col("hostname") =!= "").as[AccessLogRecWithDate]

    /**
     * Calculate the total access count based on hostname + url + accessdate.
     * The columns of result DS will be  (hostname,  url,  accessdate, total access count by key)
     */
    val dsAccessLogRecWithDateCount =
      dsAccessLogRecWithDate.rdd.map(rec => ((rec.hostname, rec.url, rec.accessDate), 1)) //Create map object where key is hostname + url + accessdate
        .reduceByKey((totVal, nextVal) => (totVal + nextVal)) //Calculate total access count where key is hostname + url + accessdate
        .map(logRec => (logRec._1._1, logRec._1._2, logRec._1._3, logRec._2)) //Here logRec is like ((hostname, url, accessDate), count)
        .toDF("hostname", "url", "accessDate", "count").as[AccessLogRecWithDateCount]

    if (LOG.isDebugEnabled()) {
      dsAccessLogRecWithDateCount.show();
    }

    /**
     * Now we have to group the record by accessdate and then sort in Desc order.
     */
    //Create DS where first column is key (accessDate) and value is List of AccessLogRecWithDateCount objects
    //We are going to add n top AccessLogRecWithDateCount object in the list in reduceGroups method.
    val dsRecByDate = dsAccessLogRecWithDateCount.map(
      accessLogRecWithDateCountRec => (accessLogRecWithDateCountRec.accessDate, List(accessLogRecWithDateCountRec)))

    /**
     * Get the top count by date.
     *
     * The outut format will be -
     *  column 1 = accessDate
     *  column 2 = accessDate, List of (hostname, url, date, and count)
     */
    val dfTopAccessCountWithKey = dsRecByDate.groupByKey(rec => rec._1) //Group by accessDate
      .reduceGroups((first, nextval) => { //Call reduce for each grouped based on accessDate

        //Add the new AccessLogRecWithDateCount in the list. Sort in reverse order because we need to find top topVisitorCount record.
        val lst = (first._2 ++ nextval._2).sortBy(_.count)(Ordering[Integer].reverse)

        //If length of the list below topVisitorCount then return new list.
        if (lst.length < topVisitorCount) {
          (first._1, lst)
        } else {
          //if list length more that topVisitorCount. Then only return topVisitorCount numbers of objects.
          (first._1, lst.take(topVisitorCount))
        }
      })

    if (LOG.isDebugEnabled()) {
      dfTopAccessCountWithKey.printSchema()
      dfTopAccessCountWithKey.show()
    }

    //Transform to final form where each item will be like - (hostname, url, date, and count) group by date and top n record.
    val dsFinal = dfTopAccessCountWithKey
      .map(m => m._2._2).toDF("rec") //get the Array object of second column. i.e list of (hostname, url, date, and count)
      .select(explode($"rec") as "recAfterTranspose")
      .select("recAfterTranspose.hostname", "recAfterTranspose.url", "recAfterTranspose.accessDate", "recAfterTranspose.count") //Collect necessary columns after explode
      .toDF().as[AccessLogRecWithDateCount]

    if (LOG.isDebugEnabled()) {
      dsFinal.show();
    }

    dsFinal
  }

  /**
   * This will convert log record to AccessLogRec case class.
   * Sample Record  - ix-esc-ca2-07.ix.netcom.com - - [01/Aug/1995:00:00:09 -0400] "GET /images/launch-logo.gif HTTP/1.0" 200 1713
   *
   * @param logLine Data format of logLine
   * ( This can also be found in https://httpd.apache.org/docs/2.4/logs.html) -
   * 		a) visitor ID
   * 		b) -
   * 		c) -
   * 		d) [ access day in the format of dd/mmm/yyyy:mm:hh:ss -zone]
   * 		e) url called
   * 		f) Status from server
   * 		g) Size of the returned object
   *
   * @return AccessLogRec containing, hostname, dateaccess and url.
   */
  def convertLogRec(logLine: String): AccessLogRec = {

    val keyValPattern = PATTERN.findFirstMatchIn(logLine)

    if (keyValPattern.isEmpty) {
      println("Rejected Log Line: " + logLine)

      //Return blank record.
      AccessLogRec("", "", "");
    } else {

      val m = keyValPattern.get

      //Populate the object and return.
      AccessLogRec(m.group(1), m.group(4), m.group(7));
    }
  }

  /**
   * Display the output in Console or save as csv file.
   *
   * if environment is local then show in console otherwise save as csv.
   *
   */
  def displayOrSaveOutput(
    isLocal:   Boolean,
    dsTopNRec: Dataset[AccessLogRecWithDateCount]) {

    //if local environment
    if (isLocal) {
      val noOfRec = dsTopNRec.count().toInt;
      //Display the record in console.
      dsTopNRec.show(noOfRec)
    } else {
      writeToCSVFile(dsTopNRec, "topAccessCount.csv")
    }
  }

  /**
   * This is the save the final record in csv file.
   *
   * NOTE : THIS WILL NOT WORK IN MY LOCAL ENVIRONMENT. SO NOT TESTED.
   */
  def writeToCSVFile(dsTopNRec: Dataset[AccessLogRecWithDateCount], csvFileName: String) {
 
    dsTopNRec.coalesce(1).write.mode(SaveMode.Overwrite).csv(csvFileName);
  }
}


