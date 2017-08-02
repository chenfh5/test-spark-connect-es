package io.github.chenfh5.hadoop_spark

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession


object SparkEnvironment {
  @transient private val sparkConf = new SparkConf()
  @transient private val sparkSession = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate()
  sparkSession.sparkContext.hadoopConfiguration.set("fs.defaultFS", "hdfs://ns3")

  def getSparkSession = this.sparkSession

}
