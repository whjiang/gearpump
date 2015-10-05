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
package io.gearpump.jarstore

import java.io.{InputStream, OutputStream}
import java.net.URI
import java.util.ServiceLoader

import akka.actor.ActorSystem
import com.typesafe.config.Config
import io.gearpump.util.{Constants, Util}

import scala.collection.JavaConverters._

case class FilePath(path: String)

case class RemoteFileInfo(fileName: String, outputStream: OutputStream)

trait JarStoreService {
  /**
    * The scheme of the JarStoreService.
   */
  val scheme : String

  /** Initialize this jar store service
    * @param config Configuration
    * @param system Actor system
    * @param jarStoreRootPath  the absolute path for a jar store */
  def init(config: Config, system: ActorSystem, jarStoreRootPath: String)

  /** Clean up the jars for an application */
  def cleanup(appId: Int)

  /**
   * This function will create an OutputStream so that Master can use to upload the client side file to jar store.
   * Master is responsible for close this OutputStream when upload done.
   * the file may be renamed by jar store, the new name is returned as part of result
   * @param appId            the application ID this file belongs to
   * @param fileName   the file name to write
   * @return  the pair (renamed file name, output stream for write)
   * */
  def createFileForWrite(appId: Int, fileName : String): RemoteFileInfo

  /**
   * This function will create an InputStream so that the file in jar store can be read
   * @param appId      the application ID this file belongs to
   * @param remoteFileName the file name in jar store
   * */
  def getInputStream(appId: Int, remoteFileName: String): InputStream
}

object JarStoreService {
  lazy val jarstoreServices: List[JarStoreService] = {
    ServiceLoader.load(classOf[JarStoreService]).asScala.toList
  }

  def get(rootPath: String): JarStoreService = {
    val scheme = new URI(Util.resolvePath(rootPath)).getScheme
    jarstoreServices.find(_.scheme == scheme).get
  }

  def get(config: Config): JarStoreService = {
    val jarStoreRootPath = config.getString(Constants.GEARPUMP_APP_JAR_STORE_ROOT_PATH)
    get(jarStoreRootPath)
  }
}

