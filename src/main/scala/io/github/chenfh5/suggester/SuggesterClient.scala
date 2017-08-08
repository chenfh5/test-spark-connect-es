package io.github.chenfh5.suggester

import scala.collection.mutable.ListBuffer

import org.elasticsearch.search.suggest.completion.CompletionSuggestionBuilder
import org.slf4j.LoggerFactory

import io.github.chenfh5.common.OwnConfigReader
import io.github.chenfh5.java_api.EsClient


object SuggesterClient {
  private val LOG = LoggerFactory.getLogger(getClass.getName)

  private val esIndex_suggest = OwnConfigReader.getOwnProperty.esIndex + "_suggest"
  private val suggesterFieldName = OwnConfigReader.getOwnProperty.suggesterFieldName
  private val suggesterSize = 11

  private val completionSuggestionBuilder = {
    val completionSuggestionBuilder = new CompletionSuggestionBuilder("temp")
    completionSuggestionBuilder.size(suggesterSize)
    completionSuggestionBuilder.field(suggesterFieldName)
  }

  def getSuggesterList(query: String) = {
    /*search builder*/
    val suggestResponse = EsClient.getEsClient
        .prepareSuggest(esIndex_suggest)
        .addSuggestion(completionSuggestionBuilder)
        .setSuggestText(query)
        .get()

    /*
        TODO:
        prefix query(suggester) between different documents should ranking,
        however here is default weight(which is 1),
        but we want suggester term can rank according to the term frequency.
    */
    /*result*/
    var output = ListBuffer[String]()
    val suggest = suggestResponse.getSuggest
    if (suggest.size() > 0) {
      val optIter = suggest.iterator().next().getEntries.get(0).iterator()
      while (optIter.hasNext) {
        output += optIter.next().getText.toString
      }
    }
    LOG.info("this is getSuggesterList={}", output)
    output.toList
  }

}
