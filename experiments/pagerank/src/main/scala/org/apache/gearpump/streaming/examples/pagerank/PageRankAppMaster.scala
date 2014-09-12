package org.apache.gearpump.streaming.examples.pagerank

import akka.actor.{ActorRef, Props}
import org.apache.gearpump.streaming.AppMaster
import org.apache.gearpump.util.Configs
import org.slf4j.{LoggerFactory, Logger}

/**
 * The app master for page rank
 */
class PageRankAppMaster(config : Configs) extends AppMaster(config) {
  private val LOG: Logger = LoggerFactory.getLogger(classOf[PageRankAppMaster])

  val totalNodeNum = config.getLong(PageNode.TOTAL_NODE_NUM)
  var pageGraphReader : ActorRef = null
  var pageGraphWriter : ActorRef = null

  override def preStart(): Unit = {
    LOG.info("Page rank master starting...")
    super.preStart()
    pageGraphReader = context.actorOf(Props(classOf[PageGraphReader], totalNodeNum), "page_graph_reader")
    pageGraphWriter = context.actorOf(Props(classOf[PageRankWriter], totalNodeNum, appId, config.getString(PageNode.MASTERS)), "page_rank_writer")
    LOG.info("page rank master started.")
  }
}
