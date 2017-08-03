import scala.collection.JavaConverters._

import org.elasticsearch.index.query.QueryBuilders
import org.elasticsearch.index.query.QueryBuilders._
import org.slf4j.LoggerFactory
import org.testng.Assert._
import org.testng.annotations.{AfterSuite, BeforeTest, Test}

import io.github.chenfh5.common.OwnCaseClass.Item
import io.github.chenfh5.java_api.EsConfiguration._
import io.github.chenfh5.java_api.{EsClient, GetFromEs}


class EsJavaClientTest {
  private val LOG = LoggerFactory.getLogger(getClass.getName)

  @BeforeTest
  def setUp(): Unit = {
    /*es configuration*/
    ips = "192.168.179.55"
  }

  @AfterSuite
  def shut(): Unit = {
    EsClient.getEsClient.close()
  }

  /*
  * directly search with exactly doc_id
  * */
  @Test(enabled = true, priority = 1)
  def clusterInfoTest() = {
    val response = EsClient.getEsClient.prepareGet()
        .setIndex(esIndex)
        .setType(esType)
        .setId("p4")
        .execute()
        .actionGet()

    val output = response.getSource
    LOG.info("this is the clusterInfoTest={}", output)
    assertTrue(output.get("dt") == "20170104")
  }

  /*
  * search with query-builder
  * */
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

    LOG.info("this is the readFromEs={}", result)
    assertTrue(result.map(_.id).max == 5)
  }

}
