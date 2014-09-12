package org.apache.gearpump.streaming.examples.pagerank

import java.io.{BufferedWriter, FileWriter}

import akka.actor.Actor
import org.apache.gearpump.cluster.client.ClientContext
import org.slf4j.{LoggerFactory, Logger}

case class NodeRank(nodeId: Long, weight: Double)
/**
 * Write the page rank result to file
 */
class PageRankWriter(totalNodeNum : Long, appId: Int, masters : String) extends Actor {
  private val LOG: Logger = LoggerFactory.getLogger(classOf[PageRankWriter])

  var outputWriter = new BufferedWriter(new FileWriter("/tmp/pagerank_result.txt"))
  var doneCount : Long = 0
  override def receive: Receive = {
    case NodeRank(nodeId, weight) => {
      val line = f"$nodeId, $weight\n"
      outputWriter.write(line)
      LOG.info(s"node $nodeId has weight $weight")
      outputWriter.flush()
      doneCount += 1
      if(doneCount >= totalNodeNum) {
        //done, shutdown
        val clientContext = ClientContext(masters)
        LOG.info(s"Shutting down application $appId")
        clientContext.shutdown(appId)
        clientContext.cleanup()
      }
    }
  }

  override def postStop(): Unit = {
    outputWriter.close()
    super.postStop()
  }
}
