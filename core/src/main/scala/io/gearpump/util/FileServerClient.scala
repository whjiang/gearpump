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

import java.io.File

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.marshalling.Marshal
import akka.http.scaladsl.model._
import akka.http.scaladsl.model.Uri.Path
import akka.http.scaladsl.unmarshalling.Unmarshal
import akka.stream.io.{SynchronousFileSource, SynchronousFileSink}
import akka.stream.scaladsl.Source
import akka.stream.scaladsl.Sink
import io.gearpump.jarstore.FilePath

import scala.concurrent.{ExecutionContext, Future}
import akka.stream._


/**
 * The Client for FileServer, Mainly used for JAR file upload/download
 */
class FileServerClient (system: ActorSystem, host: String, port: Int) {

  def this(system: ActorSystem, url: String) = {
    this(system, Uri(url).authority.host.address(), Uri(url).authority.port)
  }

  private implicit val actorSystem = system
  private implicit val materializer = ActorMaterializer()
  private implicit val ec = system.dispatcher

  val server = Uri(s"http://$host:$port")
  val httpClient = Http(system).outgoingConnection(server.authority.host.address(), server.authority.port)

  def upload(appId: Int, file: File): Future[FilePath] = {
    val target = server.withPath(Path("/upload"))

    val request = entity(appId, file).map{entity =>
      HttpRequest(HttpMethods.POST, uri = target, entity = entity)
    }

    val response = Source(request).via(httpClient).runWith(Sink.head)
    response.flatMap{some =>
      Unmarshal(some).to[String]
    }.map{path =>
      FilePath(path)
    }
  }

  def download(appId: Int, remoteFile: FilePath, saveAs: File): Future[Unit] = {
    val download = server.withPath(Path("/download")).withQuery("file" -> remoteFile.path, "appid" -> appId.toString)
    //download file to local
    val response = Source.single(HttpRequest(uri = download)).via(httpClient).runWith(Sink.head)
    val downloaded = response.flatMap { response =>
      response.entity.dataBytes.runWith(SynchronousFileSink(saveAs))
    }
    downloaded.map(written => Unit)
  }

  private def entity(appId: Int, file: File)(implicit ec: ExecutionContext): Future[RequestEntity] = {
    val entity =  HttpEntity(MediaTypes.`application/octet-stream`, file.length(),
                            SynchronousFileSource(file, chunkSize = 100000))
    val body = Source.single(
      Multipart.FormData.BodyPart(
        "uploadfile",
        entity,
        Map("filename" -> file.getName,
            "appid" -> appId.toString)))
    val form = Multipart.FormData(body)

    Marshal(form).to[RequestEntity]
  }
}
