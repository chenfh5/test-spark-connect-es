import org.slf4j.LoggerFactory
import org.testng.Assert._
import org.testng.annotations.Test

import io.github.chenfh5.lucene_analysis.CombineAnalyzer


class CombinedAnalyzerTest {
  private val LOG = LoggerFactory.getLogger(getClass.getName)

  @Test(enabled = false, priority = 1)
  def runTest1() = {
    val input = "GUCCI包包LV手袋李维斯牛仔裤"
    val output = CombineAnalyzer.getIkTokenAndPinyin(input, useSmart = true)
    LOG.info("this is the result={}", output)
    assertTrue(Set("包包", "nzk", "liweisi", "lws", "niuzaiku").subsetOf(output.toSet))
  }

  @Test(enabled = false, priority = 1)
  def runTest2() = {
    val input = "GUCCI包包LV手袋李维斯牛仔裤"
    val output = CombineAnalyzer.getIkTokenAndPinyin(input, useSmart = false)
    LOG.info("this is the result={}", output)
    assertTrue(Set("gucci", "lv", "shoudai", "nz").subsetOf(output.toSet))
  }

  @Test(enabled = false, priority = 1)
  def runTest3() = {
    val input = "People's Republic of China National Anthem zhonghuarenminguoheguoguoge 中华人民共和国国歌"
    val output = CombineAnalyzer.getIkTokenAndPinyin(input)
    LOG.info("this is the result={}", output)
    assertTrue(Set("国歌", "gg", "zhrmghg", "republic").subsetOf(output.toSet))
  }
}
