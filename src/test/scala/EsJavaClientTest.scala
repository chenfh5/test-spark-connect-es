import scala.collection.JavaConverters._

import org.elasticsearch.index.query.QueryBuilders
import org.elasticsearch.index.query.QueryBuilders._
import org.slf4j.LoggerFactory
import org.testng.annotations.{AfterSuite, BeforeTest, Test}

import io.github.chenfh5.common.OwnCaseClass.Item
import io.github.chenfh5.java_api.EsConfiguration._
import io.github.chenfh5.java_api.{EsClient, GetFromEs}


class EsJavaClientTest {
  private val LOG = LoggerFactory.getLogger(getClass.getName)

  @BeforeTest
  def setUp(): Unit = {
    /*es configuration*/
    ips = "localhost"
  }

  @AfterSuite
  def shut(): Unit = {
    EsClient.getEsClient.close()
  }

  @Test(enabled = false, priority = 1)
  def clusterInfoTest() = {
    val response = EsClient.getEsClient.prepareGet()
        .setIndex(esIndex)
        .setType(esType)
        .setId("p4")
        .execute()
        .actionGet()

    response.getSource.values().toArray.foreach(println)
    LOG.info("this is the clusterInfoTest end")

  }

  @Test(enabled = true, priority = 1)
  def readFromEs() = {
    val pageFrom = 0
    val pageSize = 3
    val queryBuilder = QueryBuilders.boolQuery().filter(matchAllQuery())
    val hits = GetFromEs.run(queryBuilder, pageFrom, pageSize)

    val result = hits.map {
      row =>
        val source = row.getSource.asScala
        Item(source.getOrElse("id", -1).asInstanceOf[Int],
          source.getOrElse("name", "-1").asInstanceOf[String],
          source.getOrElse("price", -1).asInstanceOf[Double],
          source.getOrElse("dt", "-1").asInstanceOf[String])
    }

    result.foreach(println)
    LOG.info("this is the readFromEs end")
  }

  @Test(enabled = true, priority = 1)
  def ikTest() = {

  }

}
