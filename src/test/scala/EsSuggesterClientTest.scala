import org.elasticsearch.common.settings.Settings
import org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder
import org.slf4j.LoggerFactory
import org.testng.Assert._
import org.testng.annotations.{BeforeTest, Test}

import io.github.chenfh5.common.OwnConfigReader
import io.github.chenfh5.java_api.EsClient
import io.github.chenfh5.suggester.SuggesterClient


class EsSuggesterClientTest {
  private val LOG = LoggerFactory.getLogger(getClass.getName)
  private var esIndex: String = _
  private var esType: String = _

  @BeforeTest
  def setUp(): Unit = {
    esIndex = "items"
    esType = "item_type"

    val ownProperty = OwnConfigReader.getOwnProperty
    ownProperty.esIndex = "items"
    ownProperty.esType = "item_type"
    ownProperty.suggesterFieldName = "item_name_suggester"
  }

  @Test(enabled = true, priority = 1)
  def create_index_mapping() = {
    val ind = EsClient.getEsClient.admin().indices()

    //delete index if exists
    if (EsClient.getEsClient.admin().indices().prepareExists(esIndex).get().isExists) {
      EsClient.getEsClient.admin().indices().prepareDelete(esIndex).get()
    }

    //create index
    val output = EsClient.getEsClient.admin().indices().prepareCreate(esIndex)
        .addMapping(
          esType,
          jsonBuilder()
              .startObject()
              .startObject(esType)
              .startObject("properties")
              .startObject("item_id").field("type", "Integer").endObject()
              //              .startObject("item_name").field("type", "string").endObject()
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
    * curl -XGET 'localhost:9200/items/_search?version&pretty'
    */
  @Test(enabled = true, priority = 2)
  def create_doc() = {
    val suggestList = Set("new", "newbalance", "now", "nec", "newyork", "newyear", "newhome")

    val docs = Array(
      InputJson(11, "nn1", Set("new", "newyear", "now", "nec")),
      InputJson(12, "nn2", Set("nfc", "newpower", "newarray", "newhome")),
      InputJson(13, "nn3", Set("nex", "new", "null", "newpull")),
      InputJson(14, "nn4", Set("nea", "nike", "new", "newpull")),

      InputJson(21, "nn5", Set("中", "中华", "中华网", "中华民族")),
      InputJson(22, "nn6", Set("中国", "中国共产党", "中国共产主义", "中国共产主义社会")),
      InputJson(23, "nn7", Set("中华人民共和国", "中华儿女", "中华儿童", "中华人民新世纪")),
      InputJson(24, "nn8", Set("中日友好", "中日友好医院", "中日友善", "中日友谊"))
    )
    val output = docs.foreach(insert)

    LOG.info("this is create_doc output={}", output)
    assertNotNull(output)
    LOG.info("this is create_doc end")
  }

  /**
    * ###########################################################
    * #                                                         #
    * #                  custom function                        #
    * #                                                         #
    * ###########################################################
    */
  def insert(input: InputJson) = {
    val json = jsonBuilder()
        .startObject()
        .field("item_id", input.item_id)
        //        .field("item_name", input.item_name)
        .startObject("item_name_suggester").array("input", input.item_name_suggester.toList: _*).endObject()
        .humanReadable(true)
        .endObject()

    val output = EsClient.getEsClient.prepareIndex(esIndex, esType)
        .setId(input.item_id.toString)
        .setSource(json)
        .setRefresh(true)
        .get()
    LOG.info("this is jsonBuilder={}", json.string())
    LOG.info("this is insert json into es id={}", output.getId)
  }

  @Test(enabled = true, priority = 3)
  def search_query_for_suggest() = {
    val query = "中"

    val output = SuggesterClient.getSuggesterList(query)
    assertTrue(Set("中华人民新世纪", "中国").subsetOf(output.toSet))
    LOG.info("this is search_query_for_suggest={}", output)
    LOG.info("this is search_query_for_suggest end")
  }

}

case class InputJson(
  item_id: Int,
  item_name: String,
  item_name_suggester: Set[String]
)

