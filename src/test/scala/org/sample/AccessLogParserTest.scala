package org.sample

import org.junit.Assert._
import org.junit._
import org.sample.AccessLogParser.parseLogLine

@Test
class AccessLogParserTest {

  @Test
  def testValidUrls(): Unit = {
    //Normal format with all elements
    assertEquals(parseLogLine("""cam2-2.agt.gmeds.com - - [28/Jul/1995:13:32:20 -0400] "GET /images/ksclogosmall.gif HTTP/1.0" 200 3635""")
      , AccessLog("cam2-2.agt.gmeds.com", "28/Jul/1995", "/images/ksclogosmall.gif", "200"))

    //Normal format with IPAddress
    assertEquals(parseLogLine("""199.0.2.27 - - [28/Jul/1995:13:32:20 -0400] "GET /images/NASA-logosmall.gif HTTP/1.0" 200 786""")
      , AccessLog("199.0.2.27", "28/Jul/1995", "/images/NASA-logosmall.gif", "200"))

    //Without the HTTP/1.0 protocol
    assertEquals(parseLogLine("""pipe6.nyc.pipeline.com - - [01/Jul/1995:00:22:43 -0400] "GET /shuttle/missions/sts-71/movies/sts-71-mir-dock.mpg" 200 946425"""),
      AccessLog("pipe6.nyc.pipeline.com", "01/Jul/1995", "/shuttle/missions/sts-71/movies/sts-71-mir-dock.mpg", "200"))
  }

  @Test
  def testInvalidUrls(): Unit = {
    //404 http response code and with no byte size
    assertEquals(parseLogLine("""cu-dialup-1005.cit.cornell.edu - - [01/Jul/1995:00:18:39 -0400] "GET /pub/winvn/readme.txt HTTP/1.0" 404 -"""),
    AccessLog("cu-dialup-1005.cit.cornell.edu", "01/Jul/1995", "/pub/winvn/readme.txt", "404"))

    //Bad request (http resp code is 400)
    assertEquals(parseLogLine("""klothos.crl.research.digital.com - - [10/Jul/1995:16:45:50 -0400] " A" 400 -"""),
    AccessLog("klothos.crl.research.digital.com", "10/Jul/1995", "A", "400"))

    //Incorrect request with no fields
    assertEquals(parseLogLine("""alyssa.p"""), null)
  }
}