package org.apache.gearpump.streaming.examples.pagerank

import org.apache.gearpump.Message
import org.apache.gearpump.streaming.task.{TaskContext, TaskActor}
import org.apache.gearpump.util.Configs

/**
 * A dummy node to by pass the problem in Gearpump graph representation limit. Will be removed in future.
 */
class DummyNode (conf : Configs) extends TaskActor(conf) {
  override def onStart(context: TaskContext): Unit = {
  }

  override def onNext(msg: Message): Unit = {
  }
}
