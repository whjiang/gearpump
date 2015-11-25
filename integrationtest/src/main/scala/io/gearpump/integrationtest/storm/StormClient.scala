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

package io.gearpump.integrationtest.storm


import backtype.storm.utils.DRPCClient
import io.gearpump.integrationtest.Docker
import io.gearpump.integrationtest.minicluster.BaseContainer

class StormClient(masterAddrs: Seq[(String, Int)]) {

  private val STORM_HOST = "storm0"
  private val STORM_CMD = "/opt/start storm"
  private val STORM_STARTER_JAR = "/opt/gearpump/lib/storm/storm-starter-0.9.5.jar"
  private val STORM_DRPC = "storm-drpc"
  private val CONFIG_FILE = "storm.yaml"
  private val clientContainer = new BaseContainer(STORM_HOST, STORM_DRPC, masterAddrs, Set(6627, 3772, 3773))

  def start(): Unit = {
    clientContainer.createAndStart()
  }

  def submitStormApp(mainClass: String = "",  args: String = ""): Int = {
    try {
      Docker.execAndCaptureOutput(STORM_HOST, s"$STORM_CMD -config $CONFIG_FILE " +
          s"-jar $STORM_STARTER_JAR $mainClass $args")
          .split("\n").last
          .replace("Submit application succeed. The application id is ", "")
          .toInt
    } catch {
      case ex: Throwable => -1
    }
  }

  def getDRPCClient(drpcServerIp: String): DRPCClient = {
    new DRPCClient(drpcServerIp, 3772)
  }

  def shutDown(): Unit = {
    clientContainer.killAndRemove()
  }

}
