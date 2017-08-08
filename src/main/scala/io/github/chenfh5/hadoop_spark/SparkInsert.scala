package io.github.chenfh5.hadoop_spark

import org.apache.spark.sql.Dataset
import org.elasticsearch.spark.sql.EsSparkSQL
import org.slf4j.LoggerFactory

import io.github.chenfh5.common.OwnConfigReader


object SparkInsert {
  private val LOG = LoggerFactory.getLogger(getClass.getName)

  def indexDsToEs[T](ds: Dataset[T], indexName: String) = {
    /*es configuration*/
    val eSProperties = OwnConfigReader.getOwnProperty

    //    val esConf = Map("es.nodes" -> eSProperties.ips, "es.port" -> eSProperties.thirdPartyPort, "es.mapping.id" -> "id")
    val esConf = Map("es.nodes" -> eSProperties.ips, "es.port" -> eSProperties.thirdPartyPort, "es.mapping.id" -> "item_id")
    val esIndexType = indexName + "/" + eSProperties.esType
    ds.printSchema()
    EsSparkSQL.saveToEs(ds, esIndexType, esConf)
    LOG.info("this is the indexDsToEs end")
  }

}
