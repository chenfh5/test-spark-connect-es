import scala.collection.JavaConverters._

import org.elasticsearch.index.query.QueryBuilders
import org.elasticsearch.index.query.QueryBuilders._
import org.slf4j.LoggerFactory
import org.testng.Assert._
import org.testng.annotations.{AfterSuite, BeforeTest, Test}

import io.github.chenfh5.common.OwnCaseClass.Item
import io.github.chenfh5.common.{OwnConfigReader, OwnImplicits}
import io.github.chenfh5.java_api.{EsClient, GetFromEs}


class EsJavaClientTest {
  private val LOG = LoggerFactory.getLogger(getClass.getName)
  private var esIndex: String = _
  private var esType: String = _

  @BeforeTest
  def setUp(): Unit = {
    /*es configuration*/
    val ownProperty = OwnConfigReader.getOwnProperty

    esIndex = ownProperty.esIndex
    esType = ownProperty.esType
  }

  @AfterSuite
  def shut(): Unit = {
    EsClient.getEsClient.close()
  }

  /*
  * directly search with exactly doc_id
  * after EsSparkTest priority=3
  * */
  @Test(enabled = false, priority = 4)
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
  @Test(enabled = false, priority = 4)
  def readFromEs() = {
    val pageFrom = 0
    val pageSize = 3
    val queryBuilder = QueryBuilders.boolQuery().filter(matchAllQuery())
    val hits = GetFromEs.run(queryBuilder, pageFrom, pageSize)

    import OwnImplicits._

    val result = hits.map {
      row =>
        val source = row.getSource.asScala
        Item(
          source.get("id"),
          source.get("name"),
          source.get("price"),
          source.get("dt")
        )
    }

    LOG.info("this is the readFromEs={}", result)
    assertTrue(result.map(_.id).max == 5)
  }

}
