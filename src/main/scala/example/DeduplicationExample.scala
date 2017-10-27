// Copyright (C) 2011-2012 the original author or authors.
// See the LICENCE.txt file distributed with this work for additional
// information regarding copyright ownership.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package example

import scala.concurrent.duration._

import com.typesafe.scalalogging.LazyLogging
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.Topology
import org.apache.kafka.streams.processor.{AbstractProcessor, ProcessorSupplier}
import org.apache.kafka.streams.state.{Stores, WindowStore}

object DeduplicationExample extends LazyLogging with Kafka {

  import Kafka._

  private val InputTopic = "deduplication_in"

  private val OutputTopic = "deduplication_out"

  private val DeduplicationWindow = 5.minutes

  def main(args: Array[String]): Unit = {
    kafkaStart()

    daemonThread {
      kafkaProduce { producer =>
        duplicates(producer)
      }
    }

    daemonThread {
      kafkaConsume(OutputTopic) { record =>
        logger.info(s"${record.key}: ${record.value}")
      }
    }

    daemonThread {
      Thread.sleep(3000L)

      startStreams(deduplicate())
    }

    readLine()

    kafkaStop()
  }

  def duplicates(producer: KafkaProducer[K, V]): Unit = {
    val sleep = DeduplicationWindow.toSeconds / 10

    1 to 999 foreach { i =>
      val key = "%03d".format(i)
      1 to 20 foreach { j =>
        val value = "%02d".format(j)
        producer.send(InputTopic, key, value)
        Thread.sleep(sleep)
      }
    }
  }

  def deduplicate(): Topology = {
    val ProcessorName = "deduplicator"
    val StoreName = "deduplicator-store"

    val deduplicator: ProcessorSupplier[K, V] = () => new Deduplicator(StoreName, DeduplicationWindow)

    val storeRetention = DeduplicationWindow.toMillis
    val storeWindow = DeduplicationWindow.toMillis
    val storeSegments = 3
    val storeRetainDuplicates = false

    val deduplicatorStore = Stores.windowStoreBuilder(
      Stores.persistentWindowStore(StoreName, storeRetention, storeSegments, storeWindow, storeRetainDuplicates),
      Serdes.String(),
      Serdes.String()
    ).withLoggingDisabled()

    new Topology()
      .addSource("source", InputTopic)
      .addProcessor(ProcessorName, deduplicator, "source")
      .addSink("sink", OutputTopic, ProcessorName)
      .addStateStore(deduplicatorStore, ProcessorName)
  }


  class Deduplicator(val storeName: String, val window: FiniteDuration) extends AbstractProcessor[K, V] {

    import scala.collection.JavaConverters._

    private lazy val store: WindowStore[K, V] = context().getStateStore(storeName).asInstanceOf[WindowStore[K, V]]

    override def process(key: K, value: V): Unit = {
      val timestamp = context().timestamp()
      val existingValues = store.fetch(key, timestamp - window.toMillis, timestamp).asScala

      if (existingValues.isEmpty) {
        context().forward(key, value)
        store.put(key, value)
      }

      context().commit()
    }
  }

}
