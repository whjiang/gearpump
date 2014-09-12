package org.apache.gearpump.streaming.examples.pagerank

import scala.collection.mutable
import akka.actor.Actor
import org.apache.gearpump.streaming.task.TaskId
import org.slf4j.{LoggerFactory, Logger}

case class GetNode(taskId : TaskId)
case class NodeInfo(nodeId: Long, totalNodeNum: Long, incomingNodes: Array[Long], outcomingNodes : Array[Long], weight : Double)
case class GetTaskId(nodes : Array[Long])
case class NodeTaskId(nodes : Array[TaskId])
case class NotReady()
/** A graph reader
 */
class PageGraphReader(totalNodeNum : Long) extends Actor {
  private val LOG: Logger = LoggerFactory.getLogger(classOf[PageGraphReader])
  var currentNodeId : Long = 0
  var taskIdMap = mutable.HashMap[Long, TaskId]()
  var taskId2NodeInfoMap = mutable.HashMap[TaskId, NodeInfo]()

  override def receive: Receive = {
    case getNode : GetNode =>
      val nodeInfo = taskId2NodeInfoMap.getOrElseUpdate(getNode.taskId, {
        //use similar way as Giraph simple page rank to construct graph
        //see code at https://github.com/apache/giraph/blob/release-1.0/giraph-examples/src/main/java/org/apache/giraph/examples/SimplePageRankVertex.java
        val nodeId = currentNodeId
        currentNodeId += 1
        taskIdMap.put(nodeId, getNode.taskId)

        val targetNodeId = (nodeId + 1) % totalNodeNum
        val incomingNodeId = (nodeId - 1) % totalNodeNum
        val realTargetNodeId = if (targetNodeId >= totalNodeNum) 0 else targetNodeId
        val realIncomingNodeId = if (incomingNodeId < 0) totalNodeNum - 1 else incomingNodeId
        val incomingNodes = Array.fill(1)(realIncomingNodeId)
        val outcomingNodes = Array.fill(1)(realTargetNodeId)
        val weight = nodeId * 100f
        NodeInfo(nodeId, totalNodeNum, incomingNodes, outcomingNodes, weight)
      })
      sender() ! nodeInfo

    case GetTaskId(nodes) =>
      val taskIds : Array[TaskId] = nodes.map(taskIdMap.getOrElse(_, null))
      if(taskIds.contains(null))
        sender() ! NotReady
      else
        sender() ! NodeTaskId(taskIds)
  }
}
