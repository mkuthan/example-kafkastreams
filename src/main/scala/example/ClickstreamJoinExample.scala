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

  case class EvPvKey(clientId: ClientId, pvId: PvId, evId: EvId)

  case class EvPv(evId: EvId, evValue: String, pvId: Option[PvId], pvValue: Option[String])

  private val PvTopic = "pv"

  private val EvTopic = "ev"

  private val EvPvTopic = "evpv"

  private val ClientKeySerde = KryoSerde[ClientKey]

  private val PvSerde = KryoSerde[Pv]

  private val EvPvKeySerde = KryoSerde[EvPvKey]

  private val EvPvSerde = KryoSerde[EvPv]

  private val PvWindow = 15.seconds

  private val EvPvWindow = 5.seconds

  def main(args: Array[String]): Unit = {
    kafkaStart()

    daemonThread {
      sleep(100L) // scalastyle:off
      kafkaProduce { producer =>
        clickstream(ClientKey("bob"), producer)
      }
    }

    daemonThread {
      sleep(200L) // scalastyle:off
      kafkaProduce { producer =>
        clickstream(ClientKey("jim"), producer)
      }
    }

    daemonThread {
      kafkaConsume(EvPvTopic) { record =>
        logger.info(s"${record.key}: ${record.value}")
      }
    }

    daemonThread {
      sleep(3000L) // scalastyle:off

      startStreams(smartJoin())
    }

    readLine()

    kafkaStop()
  }

  def clickstream(clientKey: ClientKey, producer: GenericProducer): Unit = {

    def sendEv(pvId: PvId, evId: EvId) =
      producer.send(EvTopic, clientKey, Ev(pvId, evId, s"ev_$evId"))

    def sendPv(pvId: PvId) =
      producer.send(PvTopic, clientKey, Pv(pvId, s"pv_$pvId"))

    1 to 999 foreach { i =>
      val pvId = "%03d".format(i)

      // early event
      sendEv(pvId, "00")
      sleep(1000L)

      // pv
      sendPv(pvId)

      // events
      sleep(1000L)
      sendEv(pvId, "01")
      sendEv(pvId, "01") // duplicate
      sleep(2000L)
      sendEv(pvId, "02")
      sendEv(pvId, "02") // duplicate
      sleep(4000L)
      sendEv(pvId, "03")
      sendEv(pvId, "03") // duplicate

      // late event
      sleep(10000L)
      sendEv(pvId, "99")
      sendEv(pvId, "99") // duplicate

      sleep(3000L)
    }
  }

  def smartJoin(): Topology = {
    val pvStoreName = "pv-store"
    val evPvStoreName = "evpv-store"
    val pvWindowProcessorName = "pv-window-processor"
    val evJoinProcessorName = "ev-join-processor"
    val evPvMapProcessorName = "ev-pv-processor"

    val pvStore = pvStoreBuilder(pvStoreName, PvWindow)
    val evPvStore = evPvStoreBuilder(evPvStoreName, EvPvWindow)

    val pvWindowProcessor: ProcessorSupplier[ClientKey, Pv] =
      () => new PvWindowProcessor(pvStoreName)

    val evJoinProcessor: ProcessorSupplier[ClientKey, Ev] =
      () => new EvJoinProcessor(pvStoreName, PvWindow, EvPvWindow)

    val evPvMapProcessor: ProcessorSupplier[EvPvKey, EvPv] =
      () => new EvPvMapProcessor()

    new Topology()
      // sources
      .addSource(PvTopic, PvTopic)
      .addSource(EvTopic, EvTopic)
      // window for pvs
      .addProcessor(pvWindowProcessorName, pvWindowProcessor, PvTopic)
      // join on clientId + pvId + evId
      .addProcessor(evJoinProcessorName, evJoinProcessor, EvTopic)
      // map join key into clientId
      .addProcessor(evPvMapProcessorName, evPvMapProcessor, evJoinProcessorName)
      // sink
      .addSink(EvPvTopic, EvPvTopic, evPvMapProcessorName)
      // state stores
      .addStateStore(pvStore, pvWindowProcessorName, evJoinProcessorName)
      .addStateStore(evPvStore, evJoinProcessorName)
  }

  def pvStoreBuilder(storeName: String, storeWindow: FiniteDuration): StoreBuilder[WindowStore[ClientKey, Pv]] = {
    val retention = storeWindow.toMillis
    val window = storeWindow.toMillis
    val segments = 3
    val retainDuplicates = false

    Stores.windowStoreBuilder(
      Stores.persistentWindowStore(storeName, retention, segments, window, retainDuplicates),
      ClientKeySerde,
      PvSerde
    )
  }

  def evPvStoreBuilder(storeName: String, storeWindow: FiniteDuration): StoreBuilder[WindowStore[EvPvKey, EvPv]] = {
    val retention = storeWindow.toMillis
    val window = storeWindow.toMillis
    val segments = 3
    val retainDuplicates = false

    Stores.windowStoreBuilder(
      Stores.persistentWindowStore(storeName, retention, segments, window, retainDuplicates),
      EvPvKeySerde,
      EvPvSerde
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

  class EvJoinProcessor(
    val pvStoreName: String,
    val joinWindow: FiniteDuration,
    val deduplicationWindow: FiniteDuration
  ) extends AbstractProcessor[ClientKey, Ev] {

    import scala.collection.JavaConverters._

    private lazy val pvStore: WindowStore[ClientKey, Pv] =
      context().getStateStore(pvStoreName).asInstanceOf[WindowStore[ClientKey, Pv]]

    private lazy val evPvStore: WindowStore[EvPvKey, EvPv] =
      context().getStateStore(pvStoreName).asInstanceOf[WindowStore[EvPvKey, EvPv]]

    override def process(key: ClientKey, ev: Ev): Unit = {
      val timestamp = context().timestamp()
      val evPvKey = EvPvKey(key.clientId, ev.pvId, ev.evId)

      if (isNotDuplicate(evPvKey, timestamp, deduplicationWindow)) {
        val pvs = storedPvs(key, timestamp, joinWindow)

        val evPvs = if (pvs.isEmpty) {
          Seq(EvPv(ev.evId, ev.value, None, None))
        } else {
          pvs
            .filter { pv =>
              pv.pvId == ev.pvId
            }
            .map { pv =>
              EvPv(ev.evId, ev.value, Some(pv.pvId), Some(pv.value))
            }
            .toSeq
        }

        evPvs.foreach { evPv =>
          context().forward(evPvKey, evPv)
          evPvStore.put(evPvKey, evPv)
        }
      }
    }

    private def isNotDuplicate(evPvKey: EvPvKey, timestamp: Long, deduplicationWindow: FiniteDuration) =
      evPvStore.fetch(evPvKey, timestamp - deduplicationWindow.toMillis, timestamp).asScala.isEmpty

    private def storedPvs(key: ClientKey, timestamp: Long, joinWindow: FiniteDuration) =
      pvStore.fetch(key, timestamp - joinWindow.toMillis, timestamp).asScala.map(_.value)
  }

  class EvPvMapProcessor extends AbstractProcessor[EvPvKey, EvPv] {
    override def process(key: EvPvKey, value: EvPv): Unit =
      context().forward(ClientKey(key.clientId), value)
  }

}
