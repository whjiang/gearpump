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

package org.apache.gearpump.streaming.examples.pipeline

import com.typesafe.config.ConfigFactory
import org.apache.gearpump.cluster.UserConfig
import org.apache.gearpump.cluster.client.ClientContext
import org.apache.gearpump.cluster.main.{ArgumentsParser, CLIOption, ParseResult}
import org.apache.gearpump.external.hbase.{HBaseDataSink, HBaseRepo, HBaseSinkInterface, HBaseSink}
import org.apache.gearpump.external.hbase.HBaseSink._
import org.apache.gearpump.partitioner.HashPartitioner
import org.apache.gearpump.streaming.kafka.KafkaSource
import org.apache.gearpump.streaming.kafka.lib.KafkaConfig
import org.apache.gearpump.streaming.sink.DataSinkProcessor
import org.apache.gearpump.streaming.source.DataSourceProcessor
import org.apache.gearpump.streaming.{Processor, StreamApplication}
import org.apache.gearpump.util.Graph._
import org.apache.gearpump.util.{Graph, LogUtil}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.HBaseConfiguration
import org.slf4j.Logger

object PipeLine extends App with ArgumentsParser {
  private val LOG: Logger = LogUtil.getLogger(getClass)
  val PROCESSORS = "pipeline.processors"
  val PERSISTORS = "pipeline.persistors"
  val HBASE_COLUMN_FAMILY = "family"
  val HBASE_COLUMN_NAME = "c1"

  override val options: Array[(String, CLIOption[Any])] = Array(
    "kafka_zookeepers" -> CLIOption[String]("Kafka zookeeper list", required = true),
    "kafka_topics" -> CLIOption[String]("Kafka topics to read", required = true),
    "kafka_parallelism" -> CLIOption[Int]("Kafka read parallelism", required = false, Some(1)),
    "hbase_table" -> CLIOption[String]("HBase table to write", required = true),
    "hbase_parallelism" -> CLIOption[Int]("Kafka write parallelism", required = false, Some(1)),
    "processor_parallelism" -> CLIOption[Int]("processing parallelism", required = false, Some(1))
  )

  def application(config: ParseResult): StreamApplication = {
    import Messages._
    val kafkaZks = config.getString("kafka_zookeepers")
    val kafkaTopic = config.getString("kafka_topics")
    val hbaseTable = config.getString("hbase_table")
    val kafkaParallelism = config.getInt("kafka_parallelism")
    val hbaseParallelism = config.getInt("hbase_parallelism")
    val processorParallelism = config.getInt("processor_parallelism")

    //create data source
    val kafkaSource = new KafkaSource(kafkaTopic, kafkaZks)
    val sourceProcessor = DataSourceProcessor(kafkaSource, kafkaParallelism, "kafkaSource")

    //create data sink
    val hbaseConf = HBaseConfiguration.create()
    val hbaseSink = new HBaseDataSink(hbaseConf, hbaseTable)
    val sinkProcessor = DataSinkProcessor(hbaseSink, hbaseParallelism, "hbaseSink")

    //create app logic
    val decodeProcessor = Processor[DecodeProcessor](processorParallelism, "CpuProcessor")
    val cpuProcessor = Processor[CpuProcessor](processorParallelism, "CpuProcessor")
    val memoryProcessor = Processor[MemoryProcessor](processorParallelism, "MemoryProcessor")

    val app = StreamApplication("PipeLine", Graph(
        sourceProcessor ~> decodeProcessor,
        decodeProcessor ~> cpuProcessor ~> sinkProcessor,
        decodeProcessor ~> memoryProcessor ~> sinkProcessor
    ), UserConfig.empty)
    app
  }

  val config = parse(args)
  val context = ClientContext()
  implicit val system = context.system
  val appId = context.submit(application(config))
  context.close()
}