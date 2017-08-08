package io.github.chenfh5.lucene_analysis

import java.io.StringReader

import scala.collection.mutable.ListBuffer

import org.apache.commons.lang3.StringUtils
import org.apache.lucene.analysis.Analyzer
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute
import org.slf4j.LoggerFactory


trait CustomAnalyzer {
  private val LOG = LoggerFactory.getLogger(getClass.getName)

  protected def getTokens(inputText: String, analyzer: Analyzer) = {
    LOG.info("this is the inputText={}", inputText)
    var tokenList = ListBuffer[String]()

    /*get token stream according to the input text*/
    val tokenStream = analyzer.tokenStream(null, new StringReader(inputText))
    val charTermAttribute = tokenStream.addAttribute(classOf[CharTermAttribute])
    tokenStream.reset()

    /*consumer the stream*/
    while (tokenStream.incrementToken()) {
      tokenList += charTermAttribute.toString
    }

    /*clean up*/
    tokenStream.end()
    tokenStream.close()

    /*return*/
    val output = tokenList.filter(StringUtils.isNotBlank(_)).distinct.toList
    LOG.info("this is the outputSet={}", output)
    output
  }

}
