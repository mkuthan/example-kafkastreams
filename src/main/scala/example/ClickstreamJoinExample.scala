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
import org.apache.kafka.streams.{StreamsBuilder, Topology}
import org.apache.kafka.streams.kstream.{JoinWindows, KeyValueMapper, KStream, Materialized, Reducer, TimeWindows, ValueJoiner, Windowed}
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

  case class PvKey(clientId: ClientId, pvId: PvId)

  case class Pv(pvId: PvId, value: String)

  case class Ev(pvId: PvId, evId: EvId, value: String)

  case class EvPvKey(clientId: ClientId, pvId: PvId, evId: EvId)

  case class EvPv(evId: EvId, evValue: String, pvId: Option[PvId], pvValue: Option[String])

  private val PvTopic = "clickstream.page_views"

  private val EvTopic = "clickstream.events"

  private val EvPvTopic = "clickstream.events_enriched"

  private val ClientKeySerde = KryoSerde[ClientKey]

  private val PvSerde = KryoSerde[Pv]

  private val EvPvKeySerde = KryoSerde[EvPvKey]

  private val EvPvSerde = KryoSerde[EvPv]

  private val PvWindow = 15.seconds

  private val EvPvWindow = 5.seconds

  def main(args: Array[String]): Unit = {
    kafkaStart()

    daemonThread {
      sleep(1.second)
      kafkaProduce { producer =>
        clickstream(ClientKey("bob"), producer)
      }
    }

    daemonThread {
      sleep(2.seconds)
      kafkaProduce { producer =>
        // clickstream(ClientKey("jim"), producer)
      }
    }

    daemonThread {
      kafkaConsume(EvPvTopic) { record =>
        logger.info(s"${record.key}: ${record.value}")
      }
    }

    daemonThread {
      sleep(3.seconds)

      startStreams(clickstreamJoinProcessorApi())
      // startStreams(clickstreamJoinDsl())
    }

    readLine()

    kafkaStop()
  }

  def clickstream(clientKey: ClientKey, producer: GenericProducer): Unit = {

    def sendPv(pvId: PvId, pvValue: String) =
      producer.send(PvTopic, clientKey, Pv(pvId, pvValue))

    def sendEv(pvId: String, evId: EvId, evValue: String) =
      producer.send(EvTopic, clientKey, Ev(pvId, evId, evValue))

    10 to 99 foreach { i =>
      val pv1Id = s"${i}_1"

      // main page view
      sendPv(pv1Id, "/")

      // a few impression events collected almost immediately
      sleep(100.millis)
      sendEv(pv1Id, "ev0", "show header")
      sendEv(pv1Id, "ev1", "show ads")
      sendEv(pv1Id, "ev2", "show recommendation")

      // single duplicated event, welcome to distributed world
      sendEv(pv1Id, "ev1", "show ads")

      // client clicks on one of the offers
      sleep(10.seconds)
      sendEv(pv1Id, "ev3", "click recommendation")

      val pv2Id = s"${i}_2"

      // out of order event collected before page view from offer page
      sendEv(pv2Id, "ev0", "show header")
      sleep(100.millis)

      // offer page view
      sendPv(pv2Id, "/offer?id=1234")

      // an impression event collected almost immediately
      sleep(100.millis)
      sendEv(pv2Id, "ev1", "show ads")

      // purchase after short coffee break (but longer than PvWindow)
      sleep(20.seconds)
      sendEv(pv1Id, "ev2", "add to cart")

      sleep(1.minute)
    }
  }

  def clickstreamJoinProcessorApi(): Topology = {
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
      () => new EvJoinProcessor(pvStoreName, evPvStoreName, PvWindow, EvPvWindow)

    val evPvMapProcessor: ProcessorSupplier[EvPvKey, EvPv] =
      () => new EvPvMapProcessor()

    new Topology()
      // sources
      .addSource(PvTopic, PvTopic)
      .addSource(EvTopic, EvTopic)
      // window for page views
      .addProcessor(pvWindowProcessorName, pvWindowProcessor, PvTopic)
      // join on (clientId + pvId + evId) and deduplicate
      .addProcessor(evJoinProcessorName, evJoinProcessor, EvTopic)
      // map key again into clientId
      .addProcessor(evPvMapProcessorName, evPvMapProcessor, evJoinProcessorName)
      // sink
      .addSink(EvPvTopic, EvPvTopic, evPvMapProcessorName)
      // state stores
      .addStateStore(pvStore, pvWindowProcessorName, evJoinProcessorName)
      .addStateStore(evPvStore, evJoinProcessorName)
  }

  def clickstreamJoinDsl(): Topology = {

    val builder = new StreamsBuilder()
    // sources
    val evStream: KStream[ClientKey, Ev] = builder.stream[ClientKey, Ev](EvTopic)
    val pvStream: KStream[ClientKey, Pv] = builder.stream[ClientKey, Pv](PvTopic)

    // repartition events by clientKey + pvKey
    val evToPvKeyMapper: KeyValueMapper[ClientKey, Ev, PvKey] =
      (clientKey, ev) => PvKey(clientKey.clientId, ev.pvId)

    val evByPvKeyStream: KStream[PvKey, Ev] = evStream.selectKey(evToPvKeyMapper)

    // repartition page views by clientKey + pvKey
    val pvToPvKeyMapper: KeyValueMapper[ClientKey, Pv, PvKey] =
      (clientKey, pv) => PvKey(clientKey.clientId, pv.pvId)

    val pvByPvKeyStream: KStream[PvKey, Pv] = pvStream.selectKey(pvToPvKeyMapper)

    // join
    val evPvJoiner: ValueJoiner[Ev, Pv, EvPv] = { (ev, pv) =>
      if (pv == null) {
        EvPv(ev.evId, ev.value, None, None)
      } else {
        EvPv(ev.evId, ev.value, Some(pv.pvId), Some(pv.value))
      }
    }

    val joinRetention = PvWindow.toMillis * 2 + 1
    val joinWindow = JoinWindows.of(PvWindow.toMillis).until(joinRetention)

    val evPvStream: KStream[PvKey, EvPv] = evByPvKeyStream.leftJoin(pvByPvKeyStream, evPvJoiner, joinWindow)

    // repartition by clientKey + pvKey + evKey
    val evPvToEvPvKeyMapper: KeyValueMapper[PvKey, EvPv, EvPvKey] =
      (pvKey, evPv) => EvPvKey(pvKey.clientId, pvKey.pvId, evPv.evId)

    val evPvByEvPvKeyStream: KStream[EvPvKey, EvPv] = evPvStream.selectKey(evPvToEvPvKeyMapper)

    // deduplicate
    val evPvReducer: Reducer[EvPv] =
      (evPv1, _) => evPv1

    val deduplicationRetention = EvPvWindow.toMillis * 2 + 1
    val deduplicationWindow = TimeWindows.of(EvPvWindow.toMillis).until(deduplicationRetention)

    val deduplicatedStream: KStream[Windowed[EvPvKey], EvPv] = evPvByEvPvKeyStream
      .groupByKey()
      .windowedBy(deduplicationWindow)
      .reduce(evPvReducer, Materialized.as("deduplication_store"))
      .toStream()

    // map key again into client id
    val evPvToClientKeyMapper: KeyValueMapper[Windowed[EvPvKey], EvPv, ClientId] =
      (windowedEvPvKey, _) => windowedEvPvKey.key.clientId

    val finalStream: KStream[ClientId, EvPv] = deduplicatedStream.selectKey(evPvToClientKeyMapper)

    // sink
    finalStream.to(EvPvTopic)

    builder.build()
  }

  def pvStoreBuilder(storeName: String, storeWindow: FiniteDuration): StoreBuilder[WindowStore[ClientKey, Pv]] = {
    import scala.collection.JavaConverters._

    val retention = storeWindow.toMillis
    val window = storeWindow.toMillis
    val segments = 3
    val retainDuplicates = true

    val loggingConfig = Map[String, String]()

    Stores.windowStoreBuilder(
      Stores.persistentWindowStore(storeName, retention, segments, window, retainDuplicates),
      ClientKeySerde,
      PvSerde
    ).withLoggingEnabled(loggingConfig.asJava)
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

    override def process(key: ClientKey, value: Pv): Unit =
      pvStore.put(key, value)
  }

  class EvJoinProcessor(
    val pvStoreName: String,
    val evPvStoreName: String,
    val joinWindow: FiniteDuration,
    val deduplicationWindow: FiniteDuration
  ) extends AbstractProcessor[ClientKey, Ev] {

    import scala.collection.JavaConverters._

    private lazy val pvStore: WindowStore[ClientKey, Pv] =
      context().getStateStore(pvStoreName).asInstanceOf[WindowStore[ClientKey, Pv]]

    private lazy val evPvStore: WindowStore[EvPvKey, EvPv] =
      context().getStateStore(evPvStoreName).asInstanceOf[WindowStore[EvPvKey, EvPv]]

    override def process(key: ClientKey, ev: Ev): Unit = {
      val timestamp = context().timestamp()
      val evPvKey = EvPvKey(key.clientId, ev.pvId, ev.evId)

      if (isNotDuplicate(evPvKey, timestamp, deduplicationWindow)) {
        val evPv = storedPvs(key, timestamp, joinWindow)
          .find { pv =>
            pv.pvId == ev.pvId
          }
          .map { pv =>
            EvPv(ev.evId, ev.value, Some(pv.pvId), Some(pv.value))
          }
          .getOrElse {
            EvPv(ev.evId, ev.value, None, None)
          }

        context().forward(evPvKey, evPv)
        evPvStore.put(evPvKey, evPv)
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
