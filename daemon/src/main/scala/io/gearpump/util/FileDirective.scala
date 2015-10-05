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

package io.gearpump.util

import akka.http.scaladsl.model.{HttpEntity, MediaTypes, Multipart}
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server._
import akka.stream.Materializer
import akka.stream.io.{InputStreamSource, OutputStreamSink}
import io.gearpump.jarstore.{JarStoreService, RemoteFileInfo}

import scala.concurrent.{ExecutionContext, Future}
import scala.util.Try


object FileDirective {

  //form field name
  type Name = String

  val CHUNK_SIZE = 262144

  case class FileInfo(originFileName: String, file: String, length: Long)

  private def uploadFileImpl(jarStore: JarStoreService)(implicit mat: Materializer, ec: ExecutionContext): Directive1[Future[Map[Name, FileInfo]]] = {
    Directive[Tuple1[Future[Map[Name, FileInfo]]]] { inner =>
      entity(as[Multipart.FormData]) { (formdata: Multipart.FormData) =>
        val fileNameMap = formdata.parts.mapAsync(1) { p =>
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
        }.runFold(Map.empty[Name, FileInfo])((set, value) => set ++ value)
        inner(Tuple1(fileNameMap))
      }
    }
  }

  def uploadFileTo(jarStore: JarStoreService): Directive1[Map[Name, FileInfo]] = {
    Directive[Tuple1[Map[Name, FileInfo]]] { inner =>
      extractMaterializer {implicit mat =>
        extractExecutionContext {implicit ec =>
          uploadFileImpl(jarStore)(mat, ec) { filesFuture =>
            ctx => {
              filesFuture.map(map => inner(Tuple1(map))).flatMap(route => route(ctx))
            }
          }
        }
      }
    }
  }

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