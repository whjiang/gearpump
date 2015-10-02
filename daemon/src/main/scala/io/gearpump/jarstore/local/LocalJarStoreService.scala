/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.gearpump.jarstore.local

import java.io._
import java.util.concurrent.TimeUnit

import akka.pattern.ask
import akka.actor.{ActorRef, ActorSystem, ActorRefFactory}
import io.gearpump.cluster.ClientToMaster.{JarStoreServerAddress, GetJarStoreServer}
import io.gearpump.cluster.master.MasterProxy
import com.typesafe.config.Config
import io.gearpump.jarstore.{RemoteFileInfo, FilePath, JarStoreService}
import io.gearpump.util._
import scala.collection.JavaConversions._
import org.slf4j.Logger

import scala.concurrent.duration.Duration
import scala.concurrent.{ExecutionContext, Await, Future}

class LocalJarStoreService extends JarStoreService{
  private def LOG: Logger = LogUtil.getLogger(getClass)
  override val scheme: String = "file"
  val defaultRootPath: Option[String] = Some("file:///var/run/gearpump/jarstore")
  var rootPath : Option[String] = None
  val random = new java.util.Random()

  /** Initialize this jar store service
    * @param config Configuration
    * @param system Actor system
    * @param jarStoreRootPath  the absolute path for a jar store */
  override def init(config: Config, system: ActorSystem, jarStoreRootPath: String): Unit = {
    rootPath = Option(jarStoreRootPath).orElse(defaultRootPath)

    //create root directory if not exist
    createDir(rootPath.get)
  }

  private def createDir(dir: String) = {
    val dirFile = new File(dir)
    if (!dirFile.exists()) {
      dirFile.mkdirs()
    }
  }

  /**
   * This function will create an OutputStream so that Master can use to upload the client side file to jar store.
   * Master is responsible for close this OutputStream when upload done.
   * the file may be renamed by jar store, the new name is returned as part of result
   * @param appId            the application ID this file belongs to
   * @param fileName   the file name to write
   * @return  the pair (renamed file name, output stream for write)
   * */
  override def createFileForWrite(appId: Int, fileName: String): RemoteFileInfo = {
    val randomInt = random.nextInt()
    val renamedName = s"$fileName$randomInt"
    val appDir = s"${rootPath.get}/app$appId"
    createDir(appDir)

    val fullPath = s"$appDir/$renamedName"
    val file = new File(fullPath)
    if(!file.exists()) {
      file.createNewFile()
    }
    RemoteFileInfo(renamedName, new FileOutputStream(file, false))
  }

  /**
   * This function will create an InputStream so that the file in jar store can be read
   * @param appId      the application ID this file belongs to
   * @param remoteFileName the file name in jar store
   **/
  override def getInputStream(appId: Int, remoteFileName: String): InputStream = {
    val fullPath = s"${rootPath.get}/app$appId/$remoteFileName"
    new BufferedInputStream(new FileInputStream(fullPath))
  }



  private implicit val timeout = Constants.FUTURE_TIMEOUT
  private var system : akka.actor.ActorSystem = null
  private var master : ActorRef = null
  private implicit def dispatcher: ExecutionContext = system.dispatcher

  def init(config: Config, system: ActorSystem): Unit = {
    this.system = system
    val masters = config.getStringList(Constants.GEARPUMP_CLUSTER_MASTERS).toList.flatMap(Util.parseHostList)
    master = system.actorOf(MasterProxy.props(masters), s"masterproxy${Util.randInt}")
  }

  private lazy val client = (master ? GetJarStoreServer).asInstanceOf[Future[JarStoreServerAddress]].map { address =>
    val client = new FileServer.Client(system, address.url)
    client
  }


  /**
   * This function will copy the remote file to local file system, called from client side.
   * @param localFile The destination of file path
   * @param remotePath The remote file path from JarStore
   */
  def copyToLocalFile(localFile: File, remotePath: FilePath): Unit = {
    LOG.info(s"Copying to local file: ${localFile.getAbsolutePath} from $remotePath")
    val future = client.flatMap(_.download(remotePath, localFile))
    Await.ready(future, Duration(60, TimeUnit.SECONDS))
  }

  /**
   * This function will copy the local file to the remote JarStore, called from client side.
   * @param localFile The local file
   */
  def copyFromLocal(localFile: File): FilePath = {
    val future = client.flatMap(_.upload(localFile))
    Await.result(future, Duration(60, TimeUnit.SECONDS))
  }
}