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
import scala.util.Random

import com.typesafe.scalalogging.LazyLogging
import org.apache.kafka.streams.Topology
import org.apache.kafka.streams.processor.{AbstractProcessor, ProcessorSupplier}
import org.apache.kafka.streams.state.{StoreBuilder, Stores, WindowStore}

object ClickstreamJoinExample extends LazyLogging with Kafka {

  import Kafka._

  type ClientId = String

  type PvId = String

  type EvId = String

  case class ClientKey(clientId: ClientId) {
    override def toString: String = clientId
  }

  case class Pv(pvId: PvId, value: String)

  case class Ev(pvId: PvId, evId: EvId, value: String)

  case class EvPvKey(clientId: ClientId, pvId: PvId, evId: EvId) {
    override def toString: String = s"$clientId: pv:$pvId ev:$evId"
  }

  case class EvPv(evValue: String, pvValue: Option[String])

  private val PvTopic = "pv"

  private val EvTopic = "ev"

  private val EvPvTopic = "evpv"

  private val PvWindow = 1.minutes

  private val clientKeySerde = new KryoSerde[ClientKey]()

  private val pvSerde = new KryoSerde[Pv]()

  def main(args: Array[String]): Unit = {
    kafkaStart()

    daemonThread {
      kafkaProduce { producer =>
        clickstream(ClientKey("bob"), producer)
      }
    }

    daemonThread {
      kafkaProduce { producer =>
        clickstream(ClientKey("jim"), producer)
      }
    }

    daemonThread {
      kafkaProduce { producer =>
        clickstream(ClientKey("sam"), producer)
      }
    }

    daemonThread {
      kafkaConsume(EvPvTopic) { record =>
        logger.info(s"${record.key}: ${record.value}")
      }
    }

    daemonThread {
      Thread.sleep(3000L)

      startStreams(smartJoin())
    }

    readLine()

    kafkaStop()
  }

  def clickstream(clientKey: ClientKey, producer: GenericProducer): Unit = {
    1 to 999 foreach { i =>
      val pvKey = "%03d".format(i)
      val pvValue = s"pv_$i"
      producer.send(PvTopic, clientKey, Pv(pvKey, pvValue))

      1 to Random.nextInt(5) foreach { j =>
        val evKey = "%01d".format(j)
        val evValue = s"ev_$i"
        producer.send(EvTopic, clientKey, Ev(pvKey, evKey, evValue))

        randomSleep(30000)
      }

      randomSleep(10000)
    }
  }

  def smartJoin(): Topology = {
    val PvStoreName = "pv-store"
    val PvWindowProcessorName = "pv-window-processor"
    val EvJoinProcessorName = "ev-join-processor"

    val pvStore = pvStoreBuilder(PvStoreName, PvWindow)

    val pvWindowProcessor: ProcessorSupplier[ClientKey, Pv] =
      () => new PvWindowProcessor(PvStoreName)

    val evJoinProcessor: ProcessorSupplier[ClientKey, Ev] =
      () => new EvJoinProcessor(PvStoreName, PvWindow)

    new Topology()
      .addSource("pv", PvTopic)
      .addSource("ev", EvTopic)
      .addProcessor(PvWindowProcessorName, pvWindowProcessor, "pv")
      .addProcessor(EvJoinProcessorName, evJoinProcessor, "ev")
      .addSink("evpv", EvPvTopic, EvJoinProcessorName)
      .addStateStore(pvStore, PvWindowProcessorName, EvJoinProcessorName)
  }

  def pvStoreBuilder(storeName: String, storeWindow: FiniteDuration): StoreBuilder[WindowStore[ClientKey, Pv]] = {
    val retention = storeWindow.toMillis
    val window = storeWindow.toMillis
    val segments = 3
    val retainDuplicates = false

    Stores.windowStoreBuilder(
      Stores.persistentWindowStore(storeName, retention, segments, window, retainDuplicates),
      clientKeySerde,
      pvSerde
    )
  }

  class PvWindowProcessor(val pvStoreName: String) extends AbstractProcessor[ClientKey, Pv] {

    private lazy val pvStore: WindowStore[ClientKey, Pv] =
      context().getStateStore(pvStoreName).asInstanceOf[WindowStore[ClientKey, Pv]]

    override def process(key: ClientKey, value: Pv): Unit = {
      context().forward(key, value)
      pvStore.put(key, value)
    }
  }

  class EvJoinProcessor(val pvStoreName: String, val window: FiniteDuration) extends AbstractProcessor[ClientKey, Ev] {

    import scala.collection.JavaConverters._

    private lazy val pvStore: WindowStore[ClientKey, Pv] =
      context().getStateStore(pvStoreName).asInstanceOf[WindowStore[ClientKey, Pv]]

    override def process(key: ClientKey, ev: Ev): Unit = {
      val timestamp = context().timestamp()
      val pvs = pvStore.fetch(key, timestamp - window.toMillis, timestamp).asScala.map(_.value)

      val evPvKey = EvPvKey(key.clientId, ev.pvId, ev.evId)

      if (pvs.isEmpty) {
        context().forward(evPvKey, EvPv(ev.value, None))
      } else {
        pvs
          .filter(_.pvId == ev.pvId)
          .foreach { pv =>
            context().forward(evPvKey, EvPv(ev.value, Some(pv.value)))
          }
      }
    }
  }


}