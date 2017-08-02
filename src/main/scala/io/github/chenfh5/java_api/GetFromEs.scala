package io.github.chenfh5.java_api

import org.elasticsearch.action.search.{SearchAction, SearchRequestBuilder}
import org.elasticsearch.common.unit.TimeValue
import org.elasticsearch.index.query.QueryBuilder
import org.elasticsearch.search.sort.SortOrder
import org.slf4j.LoggerFactory


/**
  * 1. read hits from elasticsearch
  * 2. read can use one thread only, do not need to use buck action
  * 3. need to self-build search request builder
  */
object GetFromEs {
  private val LOG = LoggerFactory.getLogger(getClass.getName)
  private val maxPageSize = 10000 //per read action if the whole hits more than 2000

  def run(queryBuilder: QueryBuilder, pageFrom: Int, pageSize: Int) = {
    require(pageSize < maxPageSize, s"Input pageSize=$pageSize can not exceed the maximum pageSize=$maxPageSize")

    val searchRequestBuilder = new SearchRequestBuilder(EsClient.getEsClient, SearchAction.INSTANCE)
        .setIndices(EsConfiguration.esIndex)
        .setTypes(EsConfiguration.esType)
        .setTimeout(TimeValue.timeValueMillis(700))
//        .setScroll(TimeValue.timeValueMillis(700)) //TODO (scroll feature should be add or not?) -> (Answer: if setting scroll, pageFrom would lose effectiveness)
        .setFrom(pageFrom * pageSize) //pageFrom = 0 is initial (zero corresponding to es configuration,To retrieve hits from a certain offset. Defaults to 0)
        .setSize(pageSize) //check this queryBuilder exceeding 10000 (The number of hits to return)
        .addSort("name", SortOrder.DESC)
        .setQuery(queryBuilder)

    LOG.info("this is the search body ====>\n{}\n<====", searchRequestBuilder.toString)

    searchRequestBuilder.get().getHits.getHits
  }

}
