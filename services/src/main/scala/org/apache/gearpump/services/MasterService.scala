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

package org.apache.gearpump.services

import akka.actor.ActorRef
import org.apache.gearpump.cluster.AppMasterToMaster.{MasterData, GetMasterData}
import org.apache.gearpump.util.{Constants}
import spray.http.StatusCodes
import spray.routing
import spray.routing.HttpService

import scala.concurrent.{Future, ExecutionContext}
import scala.util.{Failure, Success}
import akka.pattern.ask

trait MasterService extends HttpService {
  import upickle._
  def master:ActorRef

  def masterRoute: routing.Route = {
    implicit val ec: ExecutionContext = actorRefFactory.dispatcher
    implicit val timeout = Constants.FUTURE_TIMEOUT
    path("master") {
      get {
        onComplete((master ? GetMasterData).asInstanceOf[Future[MasterData]]) {
          case Success(value: MasterData) => complete(write(value))
          case Failure(ex) => complete(StatusCodes.InternalServerError, s"An error occurred: ${ex.getMessage}")
        }
      }
    }
  }
}
