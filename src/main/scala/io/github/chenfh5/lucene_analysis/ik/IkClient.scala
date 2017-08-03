package io.github.chenfh5.lucene_analysis.ik

import java.io.File

import org.apache.commons.lang3.StringUtils
import org.elasticsearch.common.settings.Settings
import org.elasticsearch.env.Environment
import org.slf4j.LoggerFactory
import org.wltea.analyzer.cfg.Configuration
import org.wltea.analyzer.lucene.IKAnalyzer

import io.github.chenfh5.lucene_analysis.CustomAnalyzer


object IkClient extends CustomAnalyzer {
  private val LOG = LoggerFactory.getLogger(getClass.getName)

  private val ikSetting = {
    val path = new File(getClass.getClassLoader.getResource("config").toURI)
    Settings.builder()
        .put("path.home", "")
        .put("path.conf", path)
        .put("use_smart", true)
  }

  private lazy val ikAnalyzerSmart = {
    val setting = ikSetting.build()
    val ikAnalyzerSmart = new IKAnalyzer(new Configuration(new Environment(setting), setting))
    LOG.info("this is the ikAnalyzerSmart={}, initialized successfully", ikAnalyzerSmart)

    ikAnalyzerSmart
  }

  private lazy val ikAnalyzerFull = {
    val setting = ikSetting
        .put("use_smart", false)
        .build()
    val ikAnalyzerFull = new IKAnalyzer(new Configuration(new Environment(setting), setting))
    LOG.info("this is the ikAnalyzerFull={}, initialized successfully", ikAnalyzerFull)

    ikAnalyzerFull
  }

  def getIkTokens(inputText: String, useSmart: Boolean = true) = {
    require(StringUtils.isNotBlank(inputText), "An input text must be specified")
    val ikAnalyzer = if (useSmart) ikAnalyzerSmart else ikAnalyzerFull

    getTokens(inputText, ikAnalyzer)
  }

}
