import java.util.concurrent.TimeUnit

import org.elasticsearch.common.settings.Settings
import org.elasticsearch.common.xcontent.XContentFactory._
import org.elasticsearch.index.query.QueryBuilders
import org.elasticsearch.index.query.QueryBuilders._
import org.slf4j.LoggerFactory
import org.testng.Assert._
import org.testng.annotations.{BeforeTest, Test}

import io.github.chenfh5.common.OwnCaseClass.{NormalJson, SuggestJson}
import io.github.chenfh5.common.{OwnConfigReader, OwnImplicits}
import io.github.chenfh5.hadoop_spark.{SparkEnvironment, SparkInsert}
import io.github.chenfh5.java_api.{EsClient, GetFromEs}
import io.github.chenfh5.lucene_analysis.CombineAnalyzer
import io.github.chenfh5.suggester.SuggesterClient


class SparkInsertJavaSearchTest {
  private val LOG = LoggerFactory.getLogger(getClass.getName)
  private final val testSwitch = true

  private var esIndex: String = _
  private var esIndex_suggest: String = _
  private var esType: String = _

  @BeforeTest
  def setUp(): Unit = {
    /*spark configuration*/
    System.setProperty("spark.app.name", "spark_connect_to_es_test")
    System.setProperty("spark.master", "local[1]")
    System.setProperty("spark.hadoop.validateOutputSpecs", "false")
    System.setProperty("spark.default.parallelism", "8")
    System.setProperty("spark.sql.shuffle.partitions", "16")

    /*es configuration*/
    esIndex = "items"
    esIndex_suggest = esIndex + "_suggest"
    esType = "item_type"

    val ownProperty = OwnConfigReader.getOwnProperty
    ownProperty.esIndex = "items"
    ownProperty.suggesterFieldName = "item_name_suggester"
    ownProperty.esType = "item_type"
  }

  @Test(enabled = testSwitch, priority = 1)
  def create_index_mapping() = {
    //delete index if exists
    if (EsClient.getEsClient.admin().indices().prepareExists(esIndex).get().isExists) {
      EsClient.getEsClient.admin().indices().prepareDelete(esIndex).get()
    }

    if (EsClient.getEsClient.admin().indices().prepareExists(esIndex_suggest).get().isExists) {
      EsClient.getEsClient.admin().indices().prepareDelete(esIndex_suggest).get()
    }

    //create normal index
    val normalIndex = EsClient.getEsClient.admin().indices().prepareCreate(esIndex)
        .addMapping(
          esType,
          jsonBuilder()
              .startObject()
              .startObject(esType)
              .startObject("properties")
              .startObject("item_id").field("type", "long").endObject()
              .startObject("item_name").field("type", "string").field("analyzer", "ik_max_word").startObject("fielddata").field("format", "paged_bytes").field("loading", "eager").endObject().endObject()
              .startObject("item_price").field("type", "float").endObject()
              .startObject("shop_name").field("type", "string").field("index", "not_analyzed").endObject()
              .endObject()
              .endObject()
              .endObject()
        )
        .setSettings(Settings.builder()
            .put("index.number_of_shards", 3)
            .put("index.number_of_replicas", 0)
            .put("index.refresh_interval", -1)
        )
        .get()

    //create suggester index
    val suggesterIndex = EsClient.getEsClient.admin().indices().prepareCreate(esIndex_suggest)
        .addMapping(
          esType,
          jsonBuilder()
              .startObject()
              .startObject(esType)
              .startObject("properties")
              .startObject("item_id").field("type", "long").endObject()
              .startObject("item_name_suggester").field("type", "completion").field("preserve_position_increments", "false").endObject()
              .endObject()
              .endObject()
              .endObject()
        )
        .setSettings(Settings.builder()
            .put("index.number_of_shards", 3)
            .put("index.number_of_replicas", 0)
            .put("index.refresh_interval", -1)
        )
        .get()

    LOG.info("this is normalIndex={}", normalIndex)
    LOG.info("this is suggesterIndex={}", suggesterIndex)
    assertNotNull(normalIndex)
    assertNotNull(suggesterIndex)
    LOG.info("this is create_index_mapping end")
  }

  @Test(enabled = testSwitch, priority = 2)
  def create_doc() = {
    val normalDocs = Seq(
      NormalJson(91, "紫魅俏运动套装女2017夏装新款薄款休闲时尚跑步服两件套T恤女字母 红色 L", 128.00, "百事天成服装专营店"),
      NormalJson(92, "⑩OSA欧莎2017秋装新款女装性感V领清新印花长袖雪纺衫C17004 森林绿色 M,OSA,,京东,网上购物", 169.00, "osa品牌服饰旗舰店"),
      NormalJson(93, "韩都衣舍2017韩版女装夏装新款宽松显瘦短袖两件套连衣裙OY6198陆 浅蓝色 M", 116.00, "韩都衣舍旗舰店"),
      NormalJson(94, "茵曼 2017夏装新款文艺T恤连衣裙套装两件套【1872250084】 牛仔蓝 M.", 199.00, "茵曼官方旗舰店"),
      NormalJson(95, "烟花烫2017秋新款女装气质外套+背心裙两件套连衣裙 山如笑 粉色 M", 298.00, "烟花烫官方旗舰店"),
      NormalJson(96, "哥弟女装2017夏季新款短袖连衣裙女纯色收腰显瘦A字型中裙590071 黑色 L(4码)", 400.00, "哥弟官方旗舰店"),
      NormalJson(97, "颜域品牌女装2017夏季新款简约短袖H型直筒乱麻拼镂空蕾丝连衣裙20S7219 黑色 XL/42", 359.00, "颜域旗舰店"),
      NormalJson(98, "[7.17]歌莉娅女装2017夏季新品短袖束腰真丝连衣裙175K4B550 H85米白底印花 S", 398.00, "歌莉娅官方旗舰店"),
      NormalJson(99, "太平鸟女装2017夏装新款印花牛仔连衣裙 白色 M", 209.00, "太平鸟女装官方旗舰店")
    )
    val suggestDocs = normalDocs.map {
      row =>
        SuggestJson(row.item_id, CombineAnalyzer.getIkTokenAndPinyin(row.item_name).toList)
    }

    val sparkSession = SparkEnvironment.getSparkSession
    import sparkSession.implicits._
    val normalDocsDs = SparkEnvironment.getSparkSession.createDataset(normalDocs)

    val suggestDocsDs = SparkEnvironment.getSparkSession.createDataset(suggestDocs)

    normalDocsDs.printSchema()
    suggestDocsDs.printSchema()

    /*insert*/
    SparkInsert.indexDsToEs(normalDocsDs, esIndex)
    SparkInsert.indexDsToEs(suggestDocsDs, esIndex_suggest)

    LOG.info("this is create_doc end")
  }

  @Test(enabled = testSwitch, priority = 3)
  def update_mapping() = {
    Thread.sleep(1000 * 2)
    val normalSetting = EsClient.getEsClient.admin().indices().prepareUpdateSettings(esIndex)
        .setSettings(Settings.builder()
            .put("index.number_of_replicas", 1)
            .put("index.refresh_interval", 10, TimeUnit.SECONDS)
        )
        .get()

    val suggestSetting = EsClient.getEsClient.admin().indices().prepareUpdateSettings(esIndex_suggest)
        .setSettings(Settings.builder()
            .put("index.number_of_replicas", 1)
            .put("index.refresh_interval", 10, TimeUnit.SECONDS)
        )
        .get()

    LOG.info("this is normalSetting={}", normalSetting)
    LOG.info("this is suggestSetting={}", suggestSetting)
    assertNotNull(normalSetting)
    assertNotNull(suggestSetting)
    LOG.info("this is update_mapping end")
  }


  @Test(enabled = testSwitch, priority = 4)
  def search_suggest_with_query() = {
    val query = "y"
    val output = SuggesterClient.getSuggesterList(query)
    assertTrue(Set("yan", "yundongtaozhuang").subsetOf(output.toSet))
    LOG.info("this is search_suggest_with_query={}, output={}", Array(query, output): _*)
    LOG.info("this is search_query_for_suggest end")
  }

  @Test(enabled = testSwitch, priority = 5)
  def search_doc_with_query() = {
    val query = "女装"
    val pageFrom = 0
    val pageSize = 11
    val queryBuilder = QueryBuilders.boolQuery().filter(termQuery("item_name", query))

    val hits = GetFromEs.run(queryBuilder, pageFrom, pageSize)

    import scala.collection.JavaConverters._

    import OwnImplicits._

    val result = hits.map {
      row =>
        val source = row.getSource.asScala
        NormalJson(
          source.get("item_id"),
          source.get("item_name"),
          source.get("item_price"),
          source.get("shop_name")
        )
    }

    LOG.info("this is the readFromEs size={}, output=\n{}", result.length, result.mkString("\n"))
  }

}



