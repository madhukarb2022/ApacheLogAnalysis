package org.sample

import org.apache.log4j.Logger
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.SparkFiles

/**
 * Main class that reads primary/secondary data source, calls the RegEx parser to extract useful fields,
 * finds the topN rows for the given field
 */
object AccessLogAnalysisMain {
  val logger: Logger = Logger.getLogger(this.getClass)
  val dataSource1 = "ftp://anonymous:guest@ita.ee.lbl.gov/traces/NASA_access_log_Jul95.gz"
  val dataSource2: String = "https://github.com/madhukarb2022/accessLogData/raw/main/NASA_access_log_Jul95.gz"
  //val dataSource2: String = "/Users/u338126/Downloads/NASA_access_log_Jul95"

  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder()
      .master("local[1]")
      .appName("ApacheAccessLogAnalysis")
      .config("spark.testing.memory", "3147480000")//Docker fix
      .getOrCreate();
    logger.info(s"APP Name: ${spark.sparkContext.appName}, Deploy Mode: ${spark.sparkContext.deployMode}" +
      s", Master: ${spark.sparkContext.master}");

    //Read the source (FTP/Github)
    val sourceDF: DataFrame = readDataSource(spark, dataSource1, dataSource2)
    logger.info("Completed reading the source file")

    //Apply RegEx parsing on the source data
    val parsedDF: DataFrame = applyRegExParser(sourceDF, spark)
    logger.info("Applied regular expressions")

    val n: Int = 3  //variable to get the top 2 rows for each date

    //First approach using window.partitionBy
    findTopNWithPartitionBy(parsedDF, n, "ipAddress", spark)
    findTopNWithPartitionBy(parsedDF, n, "endPoint", spark)

    //Second approach using df.groupBy
    findTopNWithGroupBy(parsedDF, n, "ipAddress", spark)
    findTopNWithGroupBy(parsedDF, n, "endPoint", spark)
  }

  /**
   * Applies RegEx parser on each row of the DF to extract the fields
   * @param sourceDF Source Dataframe with each row (a string) to be parsed
   * @param spark Spark session
   * @return Dataframe with 4 fields extracted for each row
   */
  private def applyRegExParser(sourceDF: DataFrame, spark: SparkSession): DataFrame = {
    import spark.implicits._
    sourceDF.map(f => {
      val str: String = f.getString(0)
      val accessLog: AccessLog = AccessLogParser.parseLogLine(str)
      if (accessLog != null)
        (accessLog.date, accessLog.ipAddress, accessLog.endpoint)
      else
        (null, str, null)
    }).toDF("accessDate", "ipAddress", "endPoint")
      .filter("accessDate is not null").cache()
  }

  /**
   * Finds the topN rows for the given field (SourceIPAddress / EndPointURL)
   * @param parsedDF Dataframe to be processed with 4 fields
   * @param n Number of records to show for each date
   * @param groupByField Field name to group by (SourceIPAddress / EndPointURL)
   * @param spark Spark session
   */
  private def findTopNWithPartitionBy(parsedDF: DataFrame, n: Int, groupByField: String, spark: SparkSession): Unit = {
    logger.info(s"Using window.partitionBy, groupByField: $groupByField, n: $n")
    import spark.implicits._
    val w1 = Window.partitionBy("accessDate", groupByField).orderBy("accessDate", groupByField)
    val w2 = Window.partitionBy("accessDate", groupByField).orderBy(desc("count"))
    val w3 = Window.partitionBy("accessDate").orderBy(desc("count"))
    val resultDF = parsedDF.withColumn("count", row_number.over(w1))
      .withColumn("rank1", rank.over(w2)).where($"rank1" === 1)
      .withColumn("rank2", rank.over(w3)).where($"rank2" <= n)
      .select("accessDate", groupByField, "count")
    //resultDF.explain("formatted")
    resultDF.show(31 * n, truncate = false) //31 is the max days in a month
  }

  /**
   * Find the top n rows using groupBy
   * @param parsedDF Dataframe to be processed with 4 fields
   * @param n Number of records to show for each date
   * @param groupByField Field name to group by (SourceIPAddress / EndPointURL)
   * @param spark Spark session
   */
  private def findTopNWithGroupBy(parsedDF: DataFrame, n: Int, groupByField: String, spark: SparkSession): Unit = {
    logger.info(s"Using dataFrame.groupBy, groupByField: $groupByField, n: $n")
    import spark.implicits._
    val w = Window.partitionBy("accessDate").orderBy(desc("count"))
    val resultDF = parsedDF.groupBy("accessDate", groupByField).agg(count(groupByField) as "count")
      .withColumn("rank", rank.over(w)).where($"rank" <= n)
    //resultDF.explain("formatted")
    resultDF.show(31 * n, truncate = false) //31 is the max days in a month
  }

  /**
   * Read the text data source using primary & alternate urls
   * @param spark Spark session
   * @param dataSource1 Primary FTP url
   * @param dataSource2 Secondary/alternate url if primary is down
   * @return dataframe with read data
   */
  def readDataSource(spark: SparkSession, dataSource1: String, dataSource2: String): DataFrame = {
    try {
      logger.info(s"Reading from primary source: $dataSource1")
      spark.read.text(dataSource1)
    } catch {
      case e: Throwable =>
        logger.info(s"Reading from secondary source: $dataSource2")
        spark.sparkContext.addFile(dataSource2)
        spark.read.text(SparkFiles.get("NASA_access_log_Jul95.gz"))
    }
  }
}
