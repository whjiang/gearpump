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
package io.gearpump.util

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.Http.ServerBinding
import akka.http.scaladsl.model.Multipart.FormData.BodyPart
import akka.http.scaladsl.model.{HttpEntity, MediaTypes, _}
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server._
import akka.stream.{Materializer, ActorMaterializer}
import akka.stream.io.{InputStreamSource, OutputStreamSink}
import io.gearpump.jarstore.{RemoteFileInfo, FilePath, JarStoreService}
import io.gearpump.util.FileDirective._
import io.gearpump.util.FileServer._
import io.gearpump.util.FileServer.Port
import spray.json.DefaultJsonProtocol._
import spray.json.JsonFormat

import scala.concurrent.{ExecutionContext, Future}
import scala.util.Try

class FileServer(system: ActorSystem, host: String, port: Int = 0, jarStore: JarStoreService) {
  import system.dispatcher
  implicit val actorSystem = system
  implicit val materializer = ActorMaterializer()
  implicit def ec: ExecutionContext = system.dispatcher

  val fileUploader = new JarStoreUploaderHandler(jarStore)

  val route: Route = {
    path("upload") {
      uploadFileTo(fileUploader) { fileMap =>
        complete(fileMap.head._2.file)
      }
    } ~
      path("download") {
        parameters("file", "appid") { (file: String, appid: String) =>
          val appId: Option[Int] = Try(appid.toInt).toOption
          downloadFile(jarStore, appId.get, file)
        }
      } ~
      pathEndOrSingleSlash {
        extractUri { uri =>
          val upload = uri.withPath(Uri.Path("/upload")).toString()
          val entity = HttpEntity(MediaTypes.`text/html`,
            s"""
            |
            |<h2>Please specify a file to upload:</h2>
            |<form action="$upload" enctype="multipart/form-data" method="post">
            |<input type="file" name="datafile" size="40">
            |</p>
            |<div>
            |<input type="submit" value="Submit">
            |</div>
            |</form>
          """.stripMargin)
        complete(entity)
      }
        }
  }

  private var connection: Future[ServerBinding] = null

  def start: Future[Port] = {
    connection = Http().bindAndHandle(Route.handlerFlow(route), host, port)
    connection.map(address => Port(address.localAddress.getPort))
  }

  def stop: Future[Unit] = {
    connection.flatMap(_.unbind())
  }
}

object FileServer {

  implicit def filePathFormat: JsonFormat[FilePath] = jsonFormat1(FilePath.apply)

  case class Port(port: Int)

  def downloadFile(jarStore: JarStoreService, appId: Int, file: String): Route = {
    val is = jarStore.getInputStream(appId, file)
    val source = InputStreamSource(()=>is, CHUNK_SIZE)

    val responseEntity = HttpEntity(
      MediaTypes.`application/octet-stream`,
      file.length,
      source)

    //TODO: close the input stream is.
    complete(responseEntity)
  }
}

/** a proxy between Jar store and HTTP directive */
class JarStoreUploaderHandler(jarStore: JarStoreService) extends FileUploadHandler {
  override def uploadFile(p: BodyPart)(implicit mat: Materializer, ec: ExecutionContext): Future[Map[Name, FileInfo]] = {
    val appIdStr = p.additionalDispositionParams.get("appid")
    val appId: Option[Int] = if(appIdStr.isEmpty) None else Try(appIdStr.get.toInt).toOption
    if (p.filename.isDefined && appId.isDefined) {
      val RemoteFileInfo(newName, os) = jarStore.createFileForWrite(appId.get, p.filename.get)
      val written = p.entity.dataBytes.runWith(OutputStreamSink(()=>os))
      written.map(written => {
        os.close()
        if (written > 0) {
          Map(p.name -> FileInfo(p.filename.get, newName, written))
        } else {
          Map.empty[Name, FileInfo]
        }
      })
    } else {
      Future(Map.empty[Name, FileInfo])
    }
  }
}