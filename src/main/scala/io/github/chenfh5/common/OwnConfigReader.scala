package io.github.chenfh5.common

import java.util.Properties


object OwnConfigReader {

  object OwnProperty {
    private val properties = {
      val properties = new Properties()
      properties.load(getClass.getResourceAsStream("/config/variable.properties"))
      properties
    }

    var clusterName = properties.getProperty("cluster.name")
    var ips = properties.getProperty("ips")
    var thirdPartyPort = properties.getProperty("rest.node.port")
    var javaClientPort = properties.getProperty("java.transport.client.port")
    var esIndex = properties.getProperty("es.index")
    var esType = properties.getProperty("es.type")
  }

  def getOwnProperty = this.OwnProperty

}
