package org.apache.gearpump.streaming.examples.pagerank

import org.apache.gearpump.cluster.client.ClientContext
import org.apache.gearpump.cluster.main.{ParseResult, CLIOption, ArgumentsParser}
import org.apache.gearpump.partitioner.{Partitioner, ShufflePartitioner}
import org.apache.gearpump.streaming.{TaskDescription, AppDescription}
import org.apache.gearpump.util.Graph._
import org.apache.gearpump.util.{LogUtil, Graph, Configs}
import org.slf4j.{Logger}

/**
 * An example class for simple page rank computation.
 * This is just a toy implementation of page rank.
 * Many considerations, e.g. page locality are not considered.
 */
object PageRank extends App with ArgumentsParser{
  private val LOG: Logger = LogUtil.getLogger(getClass)

  val INPUT_FILE = "inputFile"
  val OUTPUT_FILE = "outputFile"

  override val options: Array[(String, CLIOption[Any])] = Array(
    "master" -> CLIOption[String]("<host1:port1,host2:port2,host3:port3>", required = true),
    "runseconds" -> CLIOption[Int]("<run seconds>", required = false, defaultValue = Some(180)),
    "totalNodeNum" -> CLIOption[Long]("total node number", required = true),
    "stages" -> CLIOption[Int]("stage number", required = true))

  def application(config: ParseResult) : AppDescription = {
    val stages = config.getInt("stages")
    val totalNodeNum = config.getLong("totalNodeNum")
    val appConfig = Configs(Configs.loadApplicationConfig())
                      .withValue(PageNode.STAGES, stages)
                      .withValue(PageNode.TOTAL_NODE_NUM, totalNodeNum)
                      .withValue(PageNode.MASTERS, config.getString("master"))

    val  dag = constructDAG(totalNodeNum)
    val app = AppDescription("pagerank", classOf[PageRankAppMaster].getCanonicalName, appConfig, dag)
    app
  }

  def constructDAG(totalNodeNum : Long) : Graph[TaskDescription, Partitioner] = {
    val partitioner = new ShufflePartitioner()
    val nodeDesc = TaskDescription(classOf[PageNode].getCanonicalName, totalNodeNum.toInt)
    val dummyNodeDesc = TaskDescription(classOf[DummyNode].getCanonicalName, 1)
    val computation : Any = nodeDesc ~ partitioner ~> dummyNodeDesc
    val dag = Graph[TaskDescription, Partitioner](computation)
    dag
  }

  val config = parse(args)
  val context = ClientContext(config.getString("master"))
  val appId = context.submit(application(config))
  LOG.info(s"Page rank application submitted as application ${appId}...")
}
