package org.sample

import org.apache.log4j.Logger
import scala.util.matching.Regex

/**
 * An entry of Apache access log with 4 fields
 */
case class AccessLog(ipAddress: String,
                         date: String,
                         endpoint: String,
                         responseCode: String) {
}

object AccessLogParser {
  val logger: Logger = Logger.getLogger(this.getClass)
  val PATTERN: Regex = """^(\S+) \S+ \S+ \[(\S+):\d{2}:\d{2}:\d{2}+\s[+\-]\d{4}\] "(.+?)" (\d{3}) \S+""".r

  /**
   * Parse log message
   * @param log String Log message
   * @return Tuple with 4 fields (ipAddress, accessDate, endPoint & responseCode)
   */
  def parseLogLine(log: String): AccessLog = {

    log match {
      case PATTERN(ipAddress, date, endpoint, responseCode) =>
        var splitEndPoint:String = null
        if (endpoint != null) {
          val splitArray: Array[String] = endpoint.split(" ")
          if(splitArray.length > 1)
            splitEndPoint = splitArray(1)
        }
        AccessLog(ipAddress, date, splitEndPoint, responseCode)
      case _ => logger.error(s"Failed to parse: $log")
        null
    }
  }
}