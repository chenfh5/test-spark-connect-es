import org.slf4j.LoggerFactory
import org.testng.Assert._
import org.testng.annotations.Test

import io.github.chenfh5.lucene_analysis.ik.IkClient


class IkTest {
  private val LOG = LoggerFactory.getLogger(getClass.getName)

  @Test(enabled = false, priority = 1)
  def runTest1() = {
    val input = "中国的荣耀国人的手机4g内存最低价3200元赠送手机壳"
    val output = IkClient.getIkTokens(input)
    assertTrue(Set("中国", "荣耀", "手机", "壳").subsetOf(output))
  }

  @Test(enabled = false, priority = 1)
  def runTest2() = {
    val input = "中国的荣耀国人的手机4g内存最低价3200元赠送手机壳"
    val output = IkClient.getIkTokens(input, useSmart = false)
    assertTrue(Set("中国", "荣耀", "手机", "壳", "机壳").subsetOf(output))
  }

  @Test(enabled = false, priority = 1, expectedExceptions = Array(classOf[IllegalArgumentException]))
  def runTest3() = {
    val output = IkClient.getIkTokens("")
    LOG.info("this is the result={}", output)
  }

  @Test(enabled = false, priority = 1, expectedExceptions = Array(classOf[IllegalArgumentException], classOf[NullPointerException]))
  def runTest4() = {
    val output = IkClient.getIkTokens(null, useSmart = false)
    LOG.info("this is the result={}", output)
  }

}
