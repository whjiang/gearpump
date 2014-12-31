package org.apache.gearpump.streaming.examples.pagerank

import java.util
import java.util.concurrent.atomic.AtomicBoolean

import akka.actor.{ActorSelection, Props}
import akka.pattern.ask
import akka.util.Timeout
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global

import org.apache.gearpump.Message

import org.apache.gearpump.streaming.task._
import org.apache.gearpump.util.Configs
import org.slf4j.{LoggerFactory, Logger}

import scala.collection.mutable.ArrayBuffer
import scala.util.Try

case class InComingMessages(var count: Int, msgs: Array[Double])

case class EdgeMessage(weight : Double, sender: Long)
/**
 * Represents one node/page.
 */
class PageNode(conf : Configs) extends TaskActor(conf) {
  import org.apache.gearpump.streaming.examples.pagerank.PageNode._

  private var nodeId: Long = 0
  // outcoming nodes with NodeId
  private var outcomingNodes : Array[Long] = null
  // incoming nodes, map from NodeId -> index
  private var incomingTaskIds : Map[Long, Int] = null
  private var incomingNodeCount : Int  = 0
  private var weight : Double = 1
  private var initialized = new AtomicBoolean(false)
  private var needToCache = true
  private val totalStages : Int = conf.getInt(PageNode.STAGES)
  private var totalNodeNum : Long = 0
  private var executedStages : Int = 0
  private var incomingMsgs = ArrayBuffer[InComingMessages]()
  private val unhandledMsgQueue = ArrayBuffer[Message]()
  private implicit val timeout = Timeout(5 seconds)

  ///initialize the relationship with other nodes
  override def onStart(taskContext : TaskContext) : Unit = {
    if(initialized.get())
      return

    val graphReader = context.actorSelection(conf.appMaster.path/"page_graph_reader")
    val nodeInfo = graphReader ? GetNode(taskId)
    nodeInfo.onSuccess {
      case ni : NodeInfo =>
        incomingTaskIds = ni.incomingNodes.zipWithIndex.toMap
        incomingNodeCount = ni.incomingNodes.length
        outcomingNodes = ni.outcomingNodes
        nodeId = ni.nodeId
        totalNodeNum = ni.totalNodeNum
        weight = ni.weight

        LOG.info(s"node $nodeId with task id: $taskId and initial weight $weight, incoming nodes: "+incomingTaskIds.mkString(",")
          +"; outcoming nodes: "+outcomingNodes.mkString(","))

        initialize(graphReader)
    }

    LOG.info(s"node $taskId exit onStart.")
  }

  def initialize(graphReader : ActorSelection) : Unit = {
    if(!initialized.get()) {
      val targetTaskIdInfo = graphReader ? GetTaskId(outcomingNodes)
      targetTaskIdInfo.onSuccess {
        case NodeTaskId(nodes) =>
          outputTaskIds = nodes
          flowControl = new FlowControl(taskId, outputTaskIds.length, sessionId)
          clockTracker = new ClockTracker(flowControl)
          sendFirstAckRequests()
          initialized.set(true)

          self ! Message(EdgeMessage(Double.MaxValue, nodeId))

        case NotReady =>
          //retry
          initialize(graphReader)
      }
    }
  }

  def initOutput : Unit = {
    needToCache = false
    LOG.info(s" $nodeId initialize output.")
    //kickoff the computation of this node
    outputWeight()

    if(incomingNodeCount == 0) {
      //no need to wait, emit all messages now
      for(i <- 0 until totalStages) {
        outputWeight()
      }
    } else {
      unhandledMsgQueue.map(onNext(_))
      unhandledMsgQueue.clear()
    }
  }

  override def onNext(msg: Message): Unit = {
    LOG.info(s"node $nodeId $taskId received message: $msg.")
    msg match {
      case Message(EdgeMessage(incomingWeight, source), timestamp) => {
        if (incomingWeight == Double.MaxValue) {
          initOutput
        } else if(needToCache) {
          LOG.info(s"node $nodeId is not ready yet, cache message $msg.")
          //received message before initialized, cache it for later processing
          unhandledMsgQueue += msg
          return
        } else {
          if (executedStages >= totalStages)
            return
          val sourceTaskId = incomingTaskIds(source)
          val firstMatch = incomingMsgs.find(_.msgs(sourceTaskId) == Double.MinValue)
          val msgSet = if (firstMatch.isDefined) firstMatch.get
          else {
            //a new message arrived, need to save for later use
            val msgArray = Array.fill[Double](incomingNodeCount)(Double.MinValue)
            val newMsgSet = InComingMessages(0, msgArray)
            incomingMsgs += newMsgSet
            newMsgSet
          }
          //record this message
          msgSet.msgs(sourceTaskId) = incomingWeight
          msgSet.count = msgSet.count + 1

          if (msgSet.count == incomingNodeCount) {
            //have all incoming messages ready
            val msgs = incomingMsgs.remove(0)
            assert(msgs == msgSet)
            computeWeight(msgs.msgs)
            outputWeight()
            executedStages += 1
            if (executedStages >= totalStages) {
              //write my result and halt
              val rankWriter = context.actorSelection(conf.appMaster.path / "page_rank_writer")
              LOG.info(s"finish computing page weight. Node $nodeId has weight $weight.")
              rankWriter ! NodeRank(nodeId, weight)
            }
          }
        }
      }
    }
  }

  private def computeWeight(weights: Iterable[Double]) = {
    weight = (0.15f / totalNodeNum) + weights.sum * 0.85f
  }

  private def outputWeight() = {
    if(outputTaskIds.length > 0) {
      val msg = new EdgeMessage(weight / outputTaskIds.length, nodeId)
      LOG.info(s"$nodeId output message $msg.")
      output2AllEdges(Message(msg))
    }
  }
}

object PageNode {
  val STAGES = "stages"
  val TOTAL_NODE_NUM = "totalNodeNum"
  val MASTERS = "masters"

  private val LOG: Logger = LoggerFactory.getLogger(classOf[PageNode])
}
