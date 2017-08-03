package io.github.chenfh5.java_api

import java.net.InetAddress

import org.elasticsearch.client.transport.TransportClient
import org.elasticsearch.common.settings.Settings
import org.elasticsearch.common.transport.InetSocketTransportAddress
import org.slf4j.LoggerFactory

import io.github.chenfh5.java_api.EsConfiguration._


object EsClient {
  private val LOG = LoggerFactory.getLogger(getClass.getName)

  private val transportClient = {
    val settings = Settings
        .builder()
        .put("client.transport.sniff", true)
        .put("cluster.name", "elasticsearch")
        .build()

    val transportClient = addIps(TransportClient.builder().settings(settings).build())
    LOG.info("this is the transportClient={}, initialized successfully", transportClient)
    transportClient
  }

  private def addIps(transportClient: TransportClient) = {
    val ipArray = ips.split(",")
    for (oneIP <- ipArray) {
      transportClient
          .addTransportAddresses(new InetSocketTransportAddress(InetAddress.getByName(oneIP.trim), port))
    }
    transportClient
  }

  def getEsClient = this.transportClient

  def getBuckClient = {
    this.transportClient.prepareBulk()
  }

}
