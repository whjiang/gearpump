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
package io.gearpump.integrationtest

import io.gearpump.cluster.MasterToAppMaster
import io.gearpump.integrationtest.minicluster.MiniCluster
import org.scalatest._
import org.apache.log4j.Logger

/**
 * The abstract test spec
 */
trait TestSpecBase extends WordSpec with Matchers with BeforeAndAfter with BeforeAndAfterAll {

  private val LOG = Logger.getLogger(getClass)

  override def beforeAll(): Unit = {
    super.beforeAll()
    if (!MiniClusterProvider.managed) {
      LOG.info("Will test with a default standalone mini cluster")
      MiniClusterProvider.set(new MiniCluster).start()
    }
  }

  override def afterAll(): Unit = {
    if (!MiniClusterProvider.managed) {
      LOG.info("Will shutdown the default mini cluster")
      MiniClusterProvider.get.shutDown()
    }
    super.afterAll()
  }

  lazy val cluster = MiniClusterProvider.get
  lazy val commandLineClient = cluster.commandLineClient
  lazy val restClient = cluster.restClient

  lazy val wordCountJar = cluster.queryBuiltInExampleJars("wordcount-").head
  lazy val wordCountName = "wordCount"

  before {
    assert(cluster != null, "Configure MiniCluster properly in suite spec")
    restClient.listRunningApps().size shouldEqual 0
  }

  after {
    restClient.listRunningApps().foreach(app => {
      killAppAndVerify(app.appId)
    })
  }

  private def killAppAndVerify(appId: Int): Unit = {
    commandLineClient.killApp(appId) shouldBe true
    val actual = restClient.queryApp(appId)
    actual.appId shouldEqual appId
    actual.status shouldEqual MasterToAppMaster.AppMasterInActive
  }

}