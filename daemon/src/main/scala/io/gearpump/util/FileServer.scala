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
import akka.http.scaladsl.model.{HttpEntity, MediaTypes, _}
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server._
import akka.stream.ActorMaterializer
import io.gearpump.jarstore.{FilePath, JarStoreService}
import io.gearpump.util.FileDirective._
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

  val route: Route = {
    path("upload") {
      uploadFileTo(jarStore) { fileMap =>
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
}

