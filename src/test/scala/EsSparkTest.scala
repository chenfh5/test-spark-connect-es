import org.apache.spark.sql.functions._
import org.elasticsearch.spark.rdd.EsSpark
import org.elasticsearch.spark.sql.EsSparkSQL
import org.slf4j.LoggerFactory
import org.testng.Assert._
import org.testng.annotations.{AfterSuite, BeforeTest, Test}

import io.github.chenfh5.common.OwnCaseClass.Item
import io.github.chenfh5.common.{OwnConfigReader, OwnImplicits}
import io.github.chenfh5.hadoop_spark.SparkEnvironment


class EsSparkTest {
  private val LOG = LoggerFactory.getLogger(getClass.getName)
  private final val testSwitch = false

  private var esIndexType: String = _
  private var esConf: Map[String, String] = Map()

  @BeforeTest
  def setUp(): Unit = {
    System.setProperty("spark.app.name", "spark_connect_to_es_test")
    System.setProperty("spark.master", "local[1]")
    System.setProperty("spark.hadoop.validateOutputSpecs", "false")
    System.setProperty("spark.default.parallelism", "8")
    System.setProperty("spark.sql.shuffle.partitions", "16")

    /*es configuration*/
    val eSProperties = OwnConfigReader.getOwnProperty

    esConf +=("es.nodes" -> eSProperties.ips, "es.port" -> eSProperties.thirdPartyPort, "es.mapping.id" -> "id")
    esIndexType = "spark_index" + "/spark_type"
  }

  @AfterSuite
  def shut(): Unit = {
    SparkEnvironment.getSparkSession.stop()
  }

  @Test(enabled = testSwitch, priority = 1)
  def loadlTest() = {
    val sparkSession = SparkEnvironment.getSparkSession
    import sparkSession.implicits._

    val df = SparkEnvironment.getSparkSession.sparkContext.parallelize(Seq(
      (10, "p1", 31.9, "20170101"),
      (10, "p2", 32.9, "20170102"),
      (11, "p3", 33.9, "20170103"),
      (12, "p4", 34.9, "20170104"),
      (13, "p5", 35.9, "20170105"),
      (14, "p6", 36.9, "20170106"),
      (15, "p7", 37.9, "20170107"),
      (15, "p8", 38.9, "20170108"))
    ).toDF("id", "name", "price", "dt")

    df.show()
    assertTrue(df.agg(min("price")).head().getDouble(0) == 31.9)
  }

  @Test(enabled = testSwitch, priority = 2)
  def writeToEs() = {
    val sparkSession = SparkEnvironment.getSparkSession
    import sparkSession.implicits._

    val ds = SparkEnvironment.getSparkSession.createDataset(Seq(
      Item(0, "p1", 31.9, "20170101"),
      Item(0, "p2", 32.9, "20170102"),
      Item(1, "p3", 33.9, "20170103"),
      Item(2, "p4", 34.9, "20170104"),
      Item(3, "p5", 35.9, "20170105"),
      Item(4, "p6", 36.9, "20170106"),
      Item(5, "p7", 37.9, "20170107"),
      Item(5, "p8", 38.9, "20170108"))
    )

    esConf += ("es.mapping.id" -> "name")
    ds.printSchema()
    println(ds.schema)
    val output = EsSparkSQL.saveToEs(ds, esIndexType, esConf)
    LOG.info("this is the writeToEs end")
    assertNotNull(output)
  }

  @Test(enabled = testSwitch, priority = 3)
  def readFromEs() = {
    Thread.sleep(1000)
    val sparkSession = SparkEnvironment.getSparkSession
    import OwnImplicits._
    import sparkSession.implicits._

    val esDs = EsSpark.esRDD(SparkEnvironment.getSparkSession.sparkContext, esIndexType, esConf).map {
      row => Item(
        row._2.get("id"),
        row._2.get("name"),
        row._2.get("price"),
        row._2.get("dt")
      )
    }.toDS()

    esDs.show()
    LOG.info("this is the readFromEs end")
    assertTrue(esDs.collect().map(_.price).max == 38.9)
  }

}
