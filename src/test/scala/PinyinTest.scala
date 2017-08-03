import org.slf4j.LoggerFactory
import org.testng.Assert._
import org.testng.annotations.Test

import io.github.chenfh5.lucene_analysis.pinyin.PinyinClient


class PinyinTest {
  private val LOG = LoggerFactory.getLogger(getClass.getName)

  @Test(enabled = true, priority = 1)
  def runTest1() = {
    val input = "lv"
    val output = PinyinClient.getPinyinTokens(input)
    assertEquals(output, Set("lv"))
  }

  @Test(enabled = true, priority = 1)
  def runTest2() = {
    val input = "GUCCI包包LV手袋李维斯牛仔裤"
    val output = PinyinClient.getPinyinTokens(input)
    assertTrue(Set("gucci", "lv", "guccibblvsdlwsnzk").subsetOf(output))
  }

  @Test(enabled = true, priority = 1)
  def runTest3() = {
    val input = "iphone苹果手机"
    val output = PinyinClient.getPinyinTokens(input)
    assertTrue(Set("iphonepgsj", "pingguoshouji", "iphone").subsetOf(output))
  }

  @Test(enabled = true, priority = 1)
  def runTest4() = {
    val input = "苹果iphone手机"
    val output = PinyinClient.getPinyinTokens(input)
    assertTrue(Set("pgiphonesj", "pingguoshouji", "iphone").subsetOf(output))
  }

  @Test(enabled = true, priority = 1)
  def runTest5() = {
    val output = PinyinClient.getPinyinTokens("")
    assertEquals(output, Set())
  }

  @Test(enabled = true, priority = 1, expectedExceptions = Array(classOf[NullPointerException]))
  def runTest6() = {
    val output = PinyinClient.getPinyinTokens(null)
    LOG.info("this is the result={}", output)
  }
}
