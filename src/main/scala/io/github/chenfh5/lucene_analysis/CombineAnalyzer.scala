package io.github.chenfh5.lucene_analysis

import io.github.chenfh5.lucene_analysis.ik.IkClient
import io.github.chenfh5.lucene_analysis.pinyin.PinyinClient


object CombineAnalyzer {

  def getIkTokenAndPinyin(inputText: String, useSmart: Boolean = true): Set[String] = {
    val ikTokens = IkClient.getIkTokens(inputText, useSmart)
    val ikAndPinyinList = ikTokens.flatMap(PinyinClient.getPinyinTokens)
    ikAndPinyinList.filter(_.length > 1)
  }

}
