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

package org.apache.gearpump.cluster.client

import java.util.concurrent.TimeUnit

import akka.pattern.ask
import akka.actor.{ActorRef, ActorSystem}
import akka.util.Timeout
import org.apache.gearpump.cluster.ClientToMaster.GetJarFileContainer
import org.apache.gearpump.cluster.MasterToAppMaster.AppMastersData
import org.apache.gearpump.cluster.MasterToClient.ReplayApplicationResult
import org.apache.gearpump.cluster._
import org.apache.gearpump.cluster.master.MasterProxy
import org.apache.gearpump.jarstore.JarFileContainer
import org.apache.gearpump.transport.HostPort
import org.apache.gearpump.util.Constants._
import org.apache.gearpump.util.{LogUtil, Util}
import org.slf4j.Logger

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}

//TODO: add interface to query master here
class ClientContext(masters: Iterable[HostPort]) {
  private val LOG: Logger = LogUtil.getLogger(getClass)
  private implicit val timeout = Timeout(ClientContext.TIMEOUT, TimeUnit.SECONDS)

  private val config = ClusterConfig.load.application

  implicit val system = ActorSystem(s"client${Util.randInt}" , config)
  import system.dispatcher

  private val master = system.actorOf(MasterProxy.props(masters), MASTER)

  LOG.info(s"Creating master proxy ${master} for master list: $masters")

  /**
   * Submit an applicaiton with default jar setting. Use java property
   * "gearpump.app.jar" if defined. Otherwise, will assume the jar is on
   * the target runtime classpath, and will not send it.
   */
  def submit(app : Application) : Int = {
    submit(app, System.getProperty(GEARPUMP_APP_JAR))
  }

  def submit(app : Application, jarPath: String) : Int = {
    val client = new MasterClient(master)
    val appName = checkAndAddNamePrefix(app.name, System.getProperty(GEARPUMP_APP_NAME_PREFIX))
    val updatedApp = Application(appName, app.appMaster, app.userConfig, app.clusterConfig)
    val appJar = Option(jarPath).map(loadFile(_))
    client.submitApplication(updatedApp, appJar)
  }

  def replayFromTimestampWindowTrailingEdge(appId : Int): ReplayApplicationResult = {
    val client = new MasterClient(master)
    client.replayFromTimestampWindowTrailingEdge(appId)
  }

  def listApps: AppMastersData = {
    val client = new MasterClient(master)
    client.listApplications
  }

  def shutdown(appId : Int) : Unit = {
    val client = new MasterClient(master)
    client.shutdownApplication(appId)
  }

  def resolveAppID(appId: Int) : ActorRef = {
    val client = new MasterClient(master)
    client.resolveAppId(appId)
  }

  def close() : Unit = {
    system.shutdown()
  }

  private def loadFile(jarPath : String) : AppJar = {
    val jarFile = new java.io.File(jarPath)

    val uploadFile = (master ? GetJarFileContainer).asInstanceOf[Future[JarFileContainer]]
      .map {container =>
      container.copyFromLocal(new java.io.File(jarPath))
      AppJar(jarFile.getName, container)
    }

    Await.result(uploadFile, Duration(ClientContext.UPLOAD_FILE_TIMEOUT, TimeUnit.SECONDS))
  }

  private def checkAndAddNamePrefix(appName: String, namePrefix: String) : String = {
    val prefix = Option(namePrefix).filter(!_.isEmpty).map(_ + "_").getOrElse("")
    val fullName = prefix + appName
    if (!Util.validApplicationName(fullName)) {
      close()
      val error = s"The application name $appName is not a proper name. An app name can " +
        "be a sequence of letters, numbers or underscore character \"_\""
      throw new Exception(error)
    }
    fullName
  }
}

object ClientContext {
  val UPLOAD_FILE_TIMEOUT = 15 //15s
  val TIMEOUT = 5 //5s
  /**
   * masterList is a list of master node address
   * host1:port,host2:port2,host3:port3
   */
  def apply(masterList : String): ClientContext = {
    new ClientContext(Util.parseHostList(masterList))
  }

  def apply(masters: Iterable[HostPort]): ClientContext = new ClientContext(masters)
}
