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
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.Topology
import org.apache.kafka.streams.processor.AbstractProcessor
import org.apache.kafka.streams.processor.ProcessorSupplier
import org.apache.kafka.streams.state.StoreBuilder
import org.apache.kafka.streams.state.Stores
import org.apache.kafka.streams.state.WindowStore

object DeduplicationExample extends LazyLogging with Kafka {

  import Kafka._

  type K = String

  type V = String

  private val InputTopic = "deduplication_in"

  private val OutputTopic = "deduplication_out"

  private val DeduplicationWindow = 15.seconds

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
      sleep(3.seconds) // scalastyle:off

      startStreams(deduplicate())
    }

    readLine()

    kafkaStop()
  }

  def duplicates(producer: GenericProducer): Unit = {
    def send(k: K, v: V) =
      producer.send(InputTopic, k, s"v_$v")

    1 to 999 foreach { i =>
      val key = "%03d".format(i)

      send(key, "00")

      sleep(1.second)
      send(key, "01")

      sleep(2.seconds)
      send(key, "02")

      sleep(4.seconds)
      send(key, "03")

      // duplicates
      sleep(10.seconds)
      send(key, "99")

      sleep(5.seconds)
    }
  }

  def deduplicate(): Topology = {
    val processorName = "deduplication-processor"
    val storeName = "deduplication-store"

    val deduplicationStore =
      deduplicationStoreBuilder(storeName, DeduplicationWindow)

    val deduplicationProcessor: ProcessorSupplier[K, V] =
      () => new DeduplicationProcessor(storeName, DeduplicationWindow)

    new Topology()
      .addSource(InputTopic, InputTopic)
      .addProcessor(processorName, deduplicationProcessor, InputTopic)
      .addSink(OutputTopic, OutputTopic, processorName)
      .addStateStore(deduplicationStore, processorName)
  }

  def deduplicationStoreBuilder(storeName: String, storeWindow: FiniteDuration): StoreBuilder[WindowStore[K, V]] = {
    val retention = storeWindow.toMillis
    val window = storeWindow.toMillis
    val segments = 3
    val retainDuplicates = false

    Stores.windowStoreBuilder(
      Stores.persistentWindowStore(storeName, retention, segments, window, retainDuplicates),
      Serdes.String(),
      Serdes.String()
    )
  }

  class DeduplicationProcessor(val storeName: String, val window: FiniteDuration) extends AbstractProcessor[K, V] {

    import scala.collection.JavaConverters._

    private lazy val store: WindowStore[K, V] =
      context().getStateStore(storeName).asInstanceOf[WindowStore[K, V]]

    override def process(key: K, value: V): Unit = {
      val timestamp = context().timestamp()
      val existingValues = store.fetch(key, timestamp - window.toMillis, timestamp).asScala

      if (existingValues.isEmpty) {
        context().forward(key, value)
        store.put(key, value)
      }
    }
  }

}
