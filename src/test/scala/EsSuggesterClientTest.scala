import org.elasticsearch.common.settings.Settings
import org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder
import org.slf4j.LoggerFactory
import org.testng.Assert._
import org.testng.annotations.{BeforeTest, Test}

import io.github.chenfh5.common.OwnCaseClass.SuggestJson
import io.github.chenfh5.common.OwnConfigReader
import io.github.chenfh5.java_api.EsClient
import io.github.chenfh5.suggester.SuggesterClient


class EsSuggesterClientTest {
  private val LOG = LoggerFactory.getLogger(getClass.getName)
  private var esIndex: String = _
  private var esIndex_suggest: String = _
  private var esType: String = _

  @BeforeTest
  def setUp(): Unit = {
    esIndex = "items"
    esIndex_suggest = esIndex + "_suggest"
    esType = "item_type"

    val ownProperty = OwnConfigReader.getOwnProperty
    ownProperty.esIndex = "items"
    ownProperty.esType = "item_type"
    ownProperty.suggesterFieldName = "item_name_suggester"
  }

  @Test(enabled = false, priority = 1)
  def create_index_mapping() = {
    val ind = EsClient.getEsClient.admin().indices()

    //delete index if exists
    if (EsClient.getEsClient.admin().indices().prepareExists(esIndex_suggest).get().isExists) {
      EsClient.getEsClient.admin().indices().prepareDelete(esIndex_suggest).get()
    }

    //create index
    val output = EsClient.getEsClient.admin().indices().prepareCreate(esIndex_suggest)
        .addMapping(
          esType,
          jsonBuilder()
              .startObject()
              .startObject(esType)
              .startObject("properties")
              .startObject("item_id").field("type", "Integer").endObject()
              .startObject("item_name_suggester")
              .field("type", "completion")
              .field("preserve_position_increments", "false").endObject()
              .endObject()
              .endObject()
              .endObject()
        )
        .setSettings(Settings.builder()
            .put("index.number_of_shards", 3)
            .put("index.number_of_replicas", 0)
        )
        .get()

    LOG.info("this is create_index_mapping output={}", output)
    assertNotNull(output)
    LOG.info("this is create_index_mapping end")
  }


  /**
    * curl -XGET 'localhost:9200/_cat/indices?v'
    * curl -XGET 'localhost:9200/items/_mapping?pretty'
    * curl -XGET 'localhost:9200/items/_settings?pretty'
    * curl -XGET 'localhost:9200/items/_search?version&pretty'
    */
  @Test(enabled = false, priority = 2)
  def create_doc() = {
    val suggestList = Set("new", "newbalance", "now", "nec", "newyork", "newyear", "newhome")

    val docs = Array(
      SuggestJson(11, List("new", "newyear", "now", "nec")),
      SuggestJson(12, List("nfc", "newpower", "newarray", "newhome")),
      SuggestJson(13, List("nex", "new", "null", "newpull")),
      SuggestJson(14, List("nea", "nike", "new", "newpull")),

      SuggestJson(21, List("中", "中华", "中华网", "中华民族")),
      SuggestJson(22, List("中国", "中国共产党", "中国共产主义", "中国共产主义社会")),
      SuggestJson(23, List("中华人民共和国", "中华儿女", "中华儿童", "中华人民新世纪")),
      SuggestJson(24, List("中日友好", "中日友好医院", "中日友善", "中日友谊"))
    )
    val output = docs.foreach(insert)

    LOG.info("this is create_doc output={}", output)
    assertNotNull(output)
    LOG.info("this is create_doc end")
  }

  @Test(enabled = false, priority = 3)
  def search_query_for_suggest() = {
    val query = "中"

    val output = SuggesterClient.getSuggesterList(query)
    assertTrue(Set("中华人民新世纪", "中国").subsetOf(output.toSet))
    LOG.info("this is search_query_for_suggest={}", output)
    LOG.info("this is search_query_for_suggest end")
  }

  /**
    * ###########################################################
    * #                                                         #
    * #                  custom function                        #
    * #                                                         #
    * ###########################################################
    */
  def insert(input: SuggestJson) = {
    val json = jsonBuilder()
        .startObject()
        .field("item_id", input.item_id)
        .startObject("item_name_suggester").array("input", input.item_name_suggester.toList: _*).endObject()
        .humanReadable(true)
        .endObject()

    val output = EsClient.getEsClient.prepareIndex(esIndex_suggest, esType)
        .setId(input.item_id.toString)
        .setSource(json)
        .setRefresh(true)
        .get()
    LOG.info("this is jsonBuilder={}", json.string())
    LOG.info("this is insert json into es id={}", output.getId)
  }

}


