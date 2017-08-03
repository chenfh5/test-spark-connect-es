package io.github.chenfh5.lucene_analysis.pinyin

import org.elasticsearch.analysis.PinyinConfig
import org.elasticsearch.common.settings.Settings
import org.elasticsearch.index.analysis.PinyinAnalyzer
import org.slf4j.LoggerFactory

import io.github.chenfh5.lucene_analysis.CustomAnalyzer


object PinyinClient extends CustomAnalyzer {
  private val LOG = LoggerFactory.getLogger(getClass.getName)

  /*
  * @see
  * https://github.com/medcl/elasticsearch-analysis-pinyin/tree/v1.8.2
  * */
  private lazy val pinyinSetting = {
    Settings.builder()
        .put("keep_first_letter", true)
        .put("keep_full_pinyin", false) //necessary for my business
        .put("keep_joined_full_pinyin", true)
        .put("none_chinese_pinyin_tokenize", false)
        .put("limit_first_letter_length", 30)
        .put("keep_original", true)
  }

  private lazy val pinyinAnalyzer = {
    val setting = pinyinSetting.build()
    val pinyinAnalyzer = new PinyinAnalyzer(new PinyinConfig(setting))
    LOG.info("this is the pinyinAnalyzer={}, initialized successfully", pinyinAnalyzer)

    pinyinAnalyzer
  }

  def getPinyinTokens(inputText: String) = {
    getTokens(inputText, pinyinAnalyzer)
  }

}
