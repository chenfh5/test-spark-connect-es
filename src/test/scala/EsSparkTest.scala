import org.elasticsearch.spark.rdd.EsSpark
import org.elasticsearch.spark.sql.EsSparkSQL
import org.slf4j.LoggerFactory
import org.testng.annotations.{AfterSuite, BeforeTest, Test}

import io.github.chenfh5.common.OwnCaseClass
import io.github.chenfh5.common.OwnCaseClass.Item
import io.github.chenfh5.hadoop_spark.SparkEnvironment


class EsSparkTest {
  private val LOG = LoggerFactory.getLogger(getClass.getName)

  private final val testSwith = true
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
    esIndexType = "spark_index" + "/spark_type"
    esConf +=("es.nodes" -> "localhost", "es.port" -> "9200", "es.mapping.id" -> "id")
  }

  @AfterSuite
  def shut(): Unit = {
    SparkEnvironment.getSparkSession.stop()
  }

  @Test(enabled = false, priority = 1)
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
  }

  @Test(enabled = true, priority = 2)
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
    EsSparkSQL.saveToEs(ds, esIndexType, esConf)
    LOG.info("this is the writeToEs end")
  }

  @Test(enabled = true, priority = 3)
  def readFromEs() = {
    Thread.sleep(1000)
    val sparkSession = SparkEnvironment.getSparkSession
    import sparkSession.implicits._

    val esDs = EsSpark.esRDD(SparkEnvironment.getSparkSession.sparkContext, esIndexType, esConf).map {
      row => Item(row._2.getOrElse("id", -1).asInstanceOf[Int],
        row._2.getOrElse("name", "-1").asInstanceOf[String],
        row._2.getOrElse("price", -1).asInstanceOf[Double],
        row._2.getOrElse("dt", "-1").asInstanceOf[String])
    }.toDS()

    esDs.show()
    LOG.info("this is the readFromEs end")
  }

}
