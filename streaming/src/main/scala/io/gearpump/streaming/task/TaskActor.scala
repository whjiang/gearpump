/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.gearpump.streaming.task

import java.util
import java.util.concurrent.TimeUnit

import akka.actor._
import io.gearpump.cluster.UserConfig
import io.gearpump.gs.collections.impl.map.mutable.primitive.IntShortHashMap
import io.gearpump.metrics.Metrics
import io.gearpump.serializer.SerializationFramework
import io.gearpump.streaming.AppMasterToExecutor._
import io.gearpump.streaming.ExecutorToAppMaster._
import io.gearpump.streaming.{Constants, ProcessorId}
import io.gearpump.util.{LogUtil, TimeOutScheduler}
import io.gearpump.{Message, TimeStamp}
import org.slf4j.Logger

/**
 *
 * All tasks of Gearpump runs inside a Actor.
 * TaskActor is the Actor container for a task.
 */
class TaskActor(
    val taskId: TaskId,
    val taskContextData : TaskContextData,
    userConf : UserConfig,
    val task: TaskWrapper,
    inputSerializerPool: SerializationFramework)
  extends Actor  with TimeOutScheduler{
  var upstreamMinClock: TimeStamp = 0L
  private var _minClock: TimeStamp = 0L

  def serializerPool: SerializationFramework = inputSerializerPool

  import Constants._
  import io.gearpump.streaming.task.TaskActor._
  import taskContextData._
  val config = context.system.settings.config

  val LOG: Logger = LogUtil.getLogger(getClass, app = appId, executor = executorId, task = taskId)

  //metrics
  val metricName = s"app${appId}.processor${taskId.processorId}.task${taskId.index}"
  private val receiveLatency = Metrics(context.system).histogram(s"$metricName:receiveLatency", sampleRate = 1)
  private val processTime = Metrics(context.system).histogram(s"$metricName:processTime")
  private val receiveThroughput = Metrics(context.system).meter(s"$metricName:receiveThroughput")

  private val maxPendingMessageCount = config.getInt(GEARPUMP_STREAMING_MAX_PENDING_MESSAGE_COUNT)
  private val ackOnceEveryMessageCount =  config.getInt(GEARPUMP_STREAMING_ACK_ONCE_EVERY_MESSAGE_COUNT)

  private val executor = context.parent
  var life = taskContextData.life

  //latency probe
  import context.dispatcher

  import scala.concurrent.duration._
  final val LATENCY_PROBE_INTERVAL = FiniteDuration(1, TimeUnit.SECONDS)

  // clock report interval
  final val CLOCK_REPORT_INTERVAL = FiniteDuration(1, TimeUnit.SECONDS)

  // flush interval
  final val FLUSH_INTERVAL = FiniteDuration(100, TimeUnit.MILLISECONDS)

  private val queue = new util.LinkedList[AnyRef]()

  // securityChecker will be responsible of dropping messages from
  // unknown sources
  private val securityChecker  = new SecurityChecker(taskId, self)
  private[task] var sessionId = NONE_SESSION

  //output ports
  val outputPorts : Array[OutputPort] = taskContextData.subscribers.map { portInfo =>
                                              new OutputPort(this, portInfo._1, portInfo._2)}


  final def receive : Receive = null

  task.setTaskActor(this)

  def onStart(startTime : StartTime) : Unit = {
    task.onStart(startTime)
  }

  def onNext(msg : Message) : Unit = task.onNext(msg)

  def onUnManagedMessage(msg: Any): Unit = task.receiveUnManagedMessage.apply(msg)

  def onStop() : Unit = task.onStop()

  /**
    * output via default port (the first port) to downstream by specifying a arrayIndex
    * @param arrayIndex, subscription index (not ProcessorId)
    * @param msg
    */
  def output(arrayIndex: Int, msg: Message) : Unit = {
    outputPorts(0).output(arrayIndex, msg)
  }

  /**
    * output via default port (the first port) to downstream
    * @param msg
    */
  def output(msg : Message) : Unit = {
    outputPorts(0).output(msg)
  }

  final override def postStop() : Unit = {
    onStop()
  }

  final override def preStart() : Unit = {
    val register = RegisterTask(taskId, executorId)
    LOG.info(s"$register")
    executor ! register
    context.become(waitForTaskRegistered)
  }

  def minClockAtCurrentTask: TimeStamp = {
    outputPorts.foldLeft(Long.MaxValue){ (clock, outputPort) =>
      Math.min(clock, outputPort.minClock)
    }
  }

  private def allowSendingMoreMessages(): Boolean = {
    outputPorts.forall(_.allowSendingMoreMessages())
  }

  private def doHandleMessage(): Unit = {
    var done = false

    var count = 0
    val start = System.currentTimeMillis()

    while (allowSendingMoreMessages() && !done) {
      val msg = queue.poll()
      if (msg != null) {
        msg match {
          case ack@SendAck =>
            outputPorts.foreach(_.sendAck(ack))
          case m : Message =>
            count += 1
            onNext(m)
          case other =>
            // un-managed message
            onUnManagedMessage(other)
        }
      } else {
        done = true
      }
    }

    receiveThroughput.mark(count)
    if (count > 0) {
      processTime.update((System.currentTimeMillis() - start) / count)
    }
  }

  private def onStartClock: Unit = {
    LOG.info(s"received start, clock: $upstreamMinClock, sessionId: $sessionId")
    outputPorts.foreach(_.initialize(sessionId, maxPendingMessageCount, ackOnceEveryMessageCount))

    import scala.collection.JavaConverters._
    stashQueue.asScala.foreach{item =>
      handleMessages(item.sender).apply(item.msg)
    }
    stashQueue.clear()

    // Put this as the last step so that the subscription is already initialized.
    // Message sending in current Task before onStart will not be delivered to
    // target
    onStart(new StartTime(upstreamMinClock))

    appMaster ! GetUpstreamMinClock(taskId)
    context.become(handleMessages(sender))
  }

  def waitForTaskRegistered: Receive = {
    case start@ TaskRegistered(_, sessionId, startClock) =>
      this.sessionId = sessionId
      this.upstreamMinClock = startClock
      context.become(waitForStartClock)
  }

  private val stashQueue = new util.LinkedList[MessageAndSender]()

  def waitForStartClock : Receive = {
    case start: StartTask =>
      onStartClock
    case other: AnyRef =>
      stashQueue.add(MessageAndSender(other, sender()))
  }

  def handleMessages(sender: => ActorRef): Receive = {
    case ackRequest: InitialAckRequest =>
      val ackResponse = securityChecker.handleInitialAckRequest(ackRequest)
      if (null != ackResponse) {
        queue.add(SendAck(ackResponse, ackRequest.taskId))
        doHandleMessage()
      }
    case ackRequest: AckRequest =>
      //enqueue to handle the ackRequest and send back ack later
      val ackResponse = securityChecker.generateAckResponse(ackRequest, sender, ackOnceEveryMessageCount)
      if (null != ackResponse) {
        queue.add(SendAck(ackResponse, ackRequest.taskId))
        doHandleMessage()
      }
    case ack: Ack =>
      outputPorts.foreach(_.receiveAck(ack))
      doHandleMessage()
    case inputMessage: SerializedMessage =>
      val message = Message(serializerPool.get().deserialize(inputMessage.bytes), inputMessage.timeStamp)
      receiveMessage(message, sender)
    case inputMessage: Message =>
      receiveMessage(inputMessage, sender)
    case upstream@ UpstreamMinClock(upstreamClock) =>
      this.upstreamMinClock = upstreamClock

      val subMinClock = subscriptions.foldLeft(Long.MaxValue) { (min, sub) =>
        val subMin = sub._2.minClock
        // a subscription is holding back the _minClock;
        // we send AckRequest to its tasks to push _minClock forward
        if (subMin == _minClock) {
          sub._2.sendAckRequestOnStallingTime(_minClock)
        }
        Math.min(min, subMin)
      }

      _minClock = Math.max(life.birth, Math.min(upstreamMinClock, subMinClock))

      val update = UpdateClock(taskId, _minClock)
      context.system.scheduler.scheduleOnce(CLOCK_REPORT_INTERVAL) {
        appMaster ! update
      }

      // check whether current task is dead.
      if (_minClock > life.death) {
        // There will be no more message received...
        val unRegister = UnRegisterTask(taskId, executorId)
        executor ! unRegister

        LOG.info(s"Sending $unRegister, current minclock: ${_minClock}, life: $life")
      }

    case ChangeTask(_, dagVersion, life, subscribers) =>
      this.life = life
      subscribers.groupBy(_._1).foreach{ kv =>
        val port: String = kv._1
        val subs: Array[Subscriber] = kv._2.flatMap(_._2)
        val ports = outputPorts.filter(_.portName==port)
        if(ports.isEmpty) {
          LOG.warn(s"Invalid port name $port. Can't find from existing port list. Processor port name can't be changed at runtime.")
        }else {
          val outputPort = ports(0)
          outputPort.changeSubscription(subs, maxPendingMessageCount, ackOnceEveryMessageCount)
        }
      }
      sender ! TaskChanged(taskId, dagVersion)
    case LatencyProbe(timeStamp) =>
      receiveLatency.update(System.currentTimeMillis() - timeStamp)
    case send: SendMessageLoss =>
      LOG.info("received SendMessageLoss")
      throw new MsgLostException
    case other: AnyRef =>
      queue.add(other)
      doHandleMessage()
  }

  /**
   * @return min clock of this task
   */
  def minClock: TimeStamp = _minClock

  /**
   * @return min clock of upstream task
   */
  def getUpstreamMinClock: TimeStamp = upstreamMinClock

  private def receiveMessage(msg: Message, sender: ActorRef): Unit = {
    val messageAfterCheck = securityChecker.checkMessage(msg, sender)
    messageAfterCheck match {
      case Some(msg) =>
        queue.add(msg)
        doHandleMessage()
      case None =>
        //Todo: Indicate the error and avoid the LOG flood
        //LOG.error(s"Task $taskId drop message $msg")
    }
  }
}

object TaskActor {
  val CLOCK_SYNC_TIMEOUT_INTERVAL = 3 * 1000 //3 seconds

  // If the message comes from an unknown source, securityChecker will drop it
  class SecurityChecker(task_id: TaskId, self : ActorRef) {

    private val LOG: Logger = LogUtil.getLogger(getClass, task = task_id)

    // Use mutable HashMap for performance optimization
    private val receivedMsgCount = new IntShortHashMap()


    // Tricky performance optimization to save memory.
    // We store the session Id in the uid of ActorPath
    // ActorPath.hashCode is same as uid.
    private def getSessionId(actor: ActorRef): Int = {
      //TODO: As method uid is protected in [akka] package. We
      // are using hashCode instead of uid.
      actor.hashCode()
    }

    def handleInitialAckRequest(ackRequest: InitialAckRequest): Ack = {
      LOG.debug(s"Handle InitialAckRequest for session $ackRequest" )
      val sessionId = ackRequest.sessionId
      if (sessionId == NONE_SESSION) {
        LOG.error(s"SessionId is not initialized, ackRequest: $ackRequest")
        null
      } else {
        receivedMsgCount.put(sessionId, 0)
        Ack(task_id, 0, 0, sessionId)
      }
    }

    def generateAckResponse(ackRequest: AckRequest, sender: ActorRef, incrementCount: Int): Ack = {
      val sessionId = ackRequest.sessionId
      if (receivedMsgCount.containsKey(sessionId)) {
        // we increment more count for each AckRequest
        // to throttle the number of unacked AckRequest
        receivedMsgCount.put(sessionId, (receivedMsgCount.get(sessionId) + incrementCount).toShort)
        Ack(task_id, ackRequest.seq, receivedMsgCount.get(sessionId), ackRequest.sessionId)
      } else {
        LOG.error(s"get unknown AckRequest $ackRequest from ${sender.toString()}")
        null
      }
    }

    // If the message comes from an unknown source, then drop it
    def checkMessage(message : Message, sender: ActorRef): Option[Message] = {
      if(sender.equals(self)){
        Some(message)
      } else {
        val sessionId = getSessionId(sender)
        if (receivedMsgCount.containsKey(sessionId)) {
          receivedMsgCount.put(sessionId, (receivedMsgCount.get(sessionId) + 1).toShort)
          Some(message)
        } else {
          // This is an illegal message,
          LOG.debug(s"received message before receive the first AckRequest, session $sessionId")
          None
        }
      }
    }
  }

  case class SendAck(ack: Ack, targetTask: TaskId)

  case object FLUSH

  val NONE_SESSION = -1

  case class MessageAndSender(msg: AnyRef, sender: ActorRef)
}
