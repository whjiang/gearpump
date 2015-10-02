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
package io.gearpump.jarstore.dfs

import java.io.InputStream
import akka.actor.ActorSystem
import com.typesafe.config.Config
import org.apache.hadoop.fs.{FileSystem, Path}
import io.gearpump.jarstore.{RemoteFileInfo, FilePath, JarStoreService}
import io.gearpump.util.LogUtil
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.permission.{FsAction, FsPermission}
import org.slf4j.Logger

class DFSJarStoreService extends JarStoreService {
  private val LOG: Logger = LogUtil.getLogger(getClass)
  var rootPath : Path = null
  override val scheme: String = "hdfs"
  var fs : FileSystem = null
  val random = new java.util.Random()


  /** Initialize this jar store service
    * @param config Configuration
    * @param system Actor system
    * @param jarStoreRootPath  the absolute path for a jar store */
  override def init(config: Config, system: ActorSystem, jarStoreRootPath: String): Unit = {
    rootPath = new Path(jarStoreRootPath)
    fs = rootPath.getFileSystem(new Configuration())
    createDir(rootPath)
  }

  private def createDir(dirPath: Path) = {
    if (!fs.exists(dirPath)) {
      fs.mkdirs(dirPath, new FsPermission(FsAction.ALL, FsAction.ALL, FsAction.ALL))
    }
  }

  /**
   * This function will create an OutputStream so that Master can use to upload the client side file to jar store.
   * Master is responsible for close this OutputStream when upload done.
   * the file may be renamed by jar store, the new name is returned as part of result
   * @param appId            the application ID this file belongs to
   * @param fileName   the file name to write
   * @return  the pair (renamed file name, output stream for write)
   **/
  override def createFileForWrite(appId: Int, fileName: String): RemoteFileInfo = {
    val randomInt = random.nextInt()
    val renamedName = s"$fileName$randomInt"

    val appDir = new Path(rootPath, s"app$appId")
    createDir(appDir)

    val filePath = new Path(appDir, renamedName)
    val outputStream = fs.create(filePath)

    RemoteFileInfo(renamedName, outputStream)
  }

  /**
   * This function will create an InputStream so that the file in jar store can be read
   * @param appId      the application ID this file belongs to
   * @param remoteFileName the file name in jar store
   **/
  override def getInputStream(appId: Int, remoteFileName: String): InputStream = {
    val appDir = new Path(rootPath, s"app$appId")
    val filePath = new Path(appDir, remoteFileName)
    fs.open(filePath)
  }
}
