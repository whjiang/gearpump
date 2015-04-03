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

package org.apache.gearpump.streaming

import akka.actor.ActorSystem
import org.apache.gearpump.cluster.client.ClientContext
import org.apache.gearpump.cluster.{Application, AppDescription, ClusterConfigSource, UserConfig}
import org.apache.gearpump.partitioner.Partitioner
import org.apache.gearpump.streaming.appmaster.AppMaster
import org.apache.gearpump.streaming.task.Task
import org.apache.gearpump.util.{Graph, ReferenceEqual}
import upickle.{Writer, Js}

import scala.language.implicitConversions
import scala.reflect._

case class ProcessorDescription(taskClass: String, parallelism : Int, description: String = "", taskConf: UserConfig = null) extends ReferenceEqual

trait Processor {
  def taskClass: Class[_ <: Task]
  def parallelism : Int
  def description: String
  def taskConf: UserConfig
}

object Processor {
  def apply[T <: Task](parallel : Int)(implicit ct: ClassTag[T]): Processor = apply[T](parallel, "", null)(ct)

  def apply[T <: Task](parallel : Int, desc: String)(implicit ct: ClassTag[T]): Processor = apply[T](parallel, desc, null)(ct)

  def apply[T <: Task](parallel : Int, desc: String, taskConfig : UserConfig)(implicit ct: ClassTag[T]): Processor = new Processor {
    def taskClass = ct.runtimeClass.asInstanceOf[Class[_ <: Task]]
    def parallelism = parallel
    def description = desc
    def taskConf = taskConfig
  }
}

case class StreamApplication(
    system: ActorSystem,
    val name : String,
    inputUserConfig: UserConfig,
    dag: Graph[Processor, Partitioner],
    val clusterConfig: ClusterConfigSource)
  extends Application {

  def appMaster : String = classOf[AppMaster].getName
  
  def userConfig: UserConfig = {
    import StreamApplication._
    implicit val system2 =  system
    val newdag = dag.mapVertex[ProcessorDescription] {processor =>
      ProcessorDescription(processor.taskClass.getName, processor.parallelism, processor.description, processor.taskConf)
    }
    inputUserConfig.withValue(StreamApplication.DAG, newdag).
      withValue(EXECUTOR_CLUSTER_CONFIG, clusterConfig)
  }
}

object StreamApplication {
  def apply(clientContext: ClientContext, name : String,
      inputUserConfig: UserConfig,
      dag: Graph[Processor, Partitioner],
      clusterConfig: ClusterConfigSource = null): StreamApplication = StreamApplication(clientContext.system, name, inputUserConfig, dag, clusterConfig)
//  /**
//   * You need to provide a implicit val system: ActorSystem to do the conversion
//   */
//  implicit def AppDescriptionToApplication(app: StreamApplication)(implicit system: ActorSystem): AppDescription = {
//    import app._
//    AppDescription(name,  classOf[AppMaster].getName, userConfig.withValue(DAG, dag).withValue(EXECUTOR_CLUSTER_CONFIG, app.clusterConfig), app.clusterConfig)
//  }
//
//  implicit def ApplicationToAppDescription(app: AppDescription)(implicit system: ActorSystem): StreamingAppDescription = {
//    if (null == app) {
//      return null
//    }
//    val config = Option(app.userConfig)
//    val graph  = config match {
//      case Some(_config) =>
//        _config.getValue[Graph[ProcessorDescription, Partitioner]](StreamingAppDescription.DAG).getOrElse(Graph.empty[ProcessorDescription, Partitioner])
//      case None =>
//        Graph.empty[ProcessorDescription,Partitioner]
//    }
//    StreamingAppDescription(app.name,  app.userConfig, graph, app.clusterConfig)
//  }


  val DAG = "DAG"
  val EXECUTOR_CLUSTER_CONFIG = "executorClusterConfig"
}

