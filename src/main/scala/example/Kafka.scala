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

import java.util.Properties

import net.manub.embeddedkafka.{EmbeddedKafka, EmbeddedKafkaConfig}
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord, KafkaConsumer}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.streams.{KafkaStreams, StreamsConfig, Topology}
import org.apache.kafka.streams.processor.FailOnInvalidTimestamp

object Kafka {

  type GenericProducer = KafkaProducer[AnyRef, AnyRef]

  type GenericConsumerRecord = ConsumerRecord[AnyRef, AnyRef]

  private val BootstrapServers = "localhost:9092"

  private val SerdeName = classOf[KryoSerde[AnyRef]].getName

  private val TimestampExtractorName = classOf[FailOnInvalidTimestamp].getName

  private lazy val producerProps = {
    val props = new Properties()
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BootstrapServers)
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, SerdeName)
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, SerdeName)
    props
  }

  private lazy val consumerProps = {
    val props = new Properties()
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BootstrapServers)
    props.put(ConsumerConfig.GROUP_ID_CONFIG, randomName())
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, SerdeName)
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, SerdeName)
    props
  }

  private lazy val streamProps = {
    val props = new Properties()
    props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, BootstrapServers)
    props.put(StreamsConfig.APPLICATION_ID_CONFIG, randomName())
    props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, SerdeName)
    props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, SerdeName)
    props.put(StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG, TimestampExtractorName)
    props
  }

  implicit class KafkaProducerOps[K, V](val producer: KafkaProducer[K, V]) {
    def send(topic: String, key: K, value: V): Unit =
      producer.send(new ProducerRecord(topic, null, skewedTimestamp(), key, value))
  }

}

trait Kafka {

  import Kafka._

  def kafkaStart(): Unit = {
    implicit val config = EmbeddedKafkaConfig(9092, 2181)
    EmbeddedKafka.start()
  }

  def kafkaStop(): Unit = {
    EmbeddedKafka.stop()
  }

  def kafkaProduce(block: GenericProducer => Unit): Unit = {
    val producer = new KafkaProducer[AnyRef, AnyRef](producerProps)
    block(producer)
  }

  def kafkaConsume(topicName: String)(block: GenericConsumerRecord => Unit): Unit = {
    import scala.collection.JavaConverters._
    import scala.concurrent.duration._

    val consumer = new KafkaConsumer[AnyRef, AnyRef](consumerProps)
    consumer.subscribe(Seq(topicName).asJava)

    while (true) {
      val recs = consumer.poll(60.seconds.toMillis)
      recs.iterator().asScala.foreach { record =>
        block(record)
      }
    }
  }

  def startStreams(topology: Topology): Unit = {
    val streams = new KafkaStreams(topology, streamProps)
    streams.cleanUp()
    streams.start()
  }

}
