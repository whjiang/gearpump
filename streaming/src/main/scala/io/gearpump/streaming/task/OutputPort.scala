package io.gearpump.streaming.task

import io.gearpump._
import io.gearpump.metrics.Metrics
import io.gearpump.streaming.ProcessorId
import io.gearpump.streaming.task.TaskActor.SendAck
import io.gearpump.util.LogUtil
import org.slf4j.Logger

/**
  * Output port for a task
  */
class OutputPort(task: TaskActor, val portName: String, subscribers: List[Subscriber]) extends ExpressTransport(task) {
  var subscriptions = List.empty[(Int, Subscription)]

  val sendThroughput = Metrics(task.context.system).meter(s"${task.metricName}.port$portName:sendThroughput")

  express.registerLocalActor(TaskId.toLong(taskId), task.self)

  /**
    * output to a downstream by specifying a arrayIndex
    * @param arrayIndex, subscription index (not ProcessorId)
    * @param msg
    */
  def output(arrayIndex: Int, msg: Message) : Unit = {
    LOG.debug("[output]: " + msg.msg)
    var count = 0
    count =  this.subscriptions(arrayIndex)._2.sendMessage(msg)
    sendThroughput.mark(count)
  }

  def output(msg : Message) : Unit = {
    LOG.debug("[output]: " + msg.msg)
    var count = 0
    this.subscriptions.foreach{ subscription =>
      count = subscription._2.sendMessage(msg)
    }
    sendThroughput.mark(count)
  }

  def initialize(sessionId: Int, maxPendingMessageCount: Int, ackOnceEveryMessageCount: Int): Unit = {
    subscriptions = subscribers.map { subscriber =>
      (subscriber.processorId ,
        new Subscription(appId, executorId, taskId, subscriber, sessionId, this,
          maxPendingMessageCount, ackOnceEveryMessageCount))
    }.sortBy(_._1)

    subscriptions.foreach(_._2.start)

    local.copy()
  }

  def minClock: TimeStamp = {
    this.subscriptions.foldLeft(Long.MaxValue){ (clock, subscription) =>
      Math.min(clock, subscription._2.minClock)
    }
  }

  def allowSendingMoreMessages(): Boolean = {
    subscriptions.forall(_._2.allowSendingMoreMessages())
  }

  def receiveAck(ack: Ack): Unit = {
    subscriptions.find(_._1 == ack.taskId.processorId).foreach(_._2.receiveAck(ack))
  }

  def changeSubscription(subscribers: Array[Subscriber], maxPendingMessageCount: Int, ackOnceEveryMessageCount: Int) : Unit = {
    subscribers.foreach { subscriber =>
      val processorId = subscriber.processorId
      val subscription = getSubscription(processorId)
      subscription match {
        case Some(subscription) =>
          subscription.changeLife(subscriber.lifeTime cross task.life)
        case None =>
          val subscription = new Subscription(appId, executorId, taskId, subscriber, task.sessionId, this,
            maxPendingMessageCount, ackOnceEveryMessageCount)
          subscription.start
          subscriptions :+= (subscriber.processorId, subscription)
          // sort, keep the order
          subscriptions = subscriptions.sortBy(_._1)
      }
    }
  }

  private def getSubscription(processorId: ProcessorId): Option[Subscription] = {
    subscriptions.find(_._1 == processorId).map(_._2)
  }

  def sendAck(ack: SendAck) = {
    if(subscriptions.filter(_._1==ack.targetTask.processorId).nonEmpty) {
      transport(ack.ack, ack.targetTask)
    }
  }
}