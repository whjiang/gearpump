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

import akka.actor.ActorSystem
import com.typesafe.config.Config
import io.gearpump.jarstore.{JarStoreService, RemoteFileInfo}
import io.gearpump.util._
import org.slf4j.Logger

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


  /** Clean up the jars for an application */
  override def cleanup(appId: Int): Unit = {
    val appDir = getAppDir(appId)
    val dirFile = new File(appDir)
    org.apache.commons.io.FileUtils.deleteDirectory(dirFile)
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
    val appDir = getAppDir(appId)
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
    val appDir = getAppDir(appId)
    val fullPath = s"$appDir/$remoteFileName"
    new BufferedInputStream(new FileInputStream(fullPath))
  }

  private def getAppDir(appId: Int) : String = {
    s"${rootPath.get}/app$appId"
  }
}