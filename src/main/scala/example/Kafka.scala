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

import scala.concurrent.duration._

import com.typesafe.scalalogging.LazyLogging
import net.manub.embeddedkafka.EmbeddedKafka
import net.manub.embeddedkafka.EmbeddedKafkaConfig
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.StreamsConfig
import org.apache.kafka.streams.Topology
import org.apache.kafka.streams.processor.FailOnInvalidTimestamp

object Kafka {

  type GenericProducer = KafkaProducer[AnyRef, AnyRef]

  type GenericConsumerRecord = ConsumerRecord[AnyRef, AnyRef]

  private val SerdeName = classOf[KryoSerde[AnyRef]].getName

  private val TimestampExtractorName = classOf[FailOnInvalidTimestamp].getName

  private val DefaultNameLeght = 10

  private val DefaultPollTime = 60.seconds

  private val DefaultCommitInterval = 5.seconds

  private val DefaultKafkaPort = 9092

  private val DefaultZkPort = 2181

  private val DefaultBootstrapServers = s"localhost:$DefaultKafkaPort"

  private lazy val ProducerProps = {
    val props = new Properties()
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, DefaultBootstrapServers)
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, SerdeName)
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, SerdeName)
    props
  }

  private lazy val ConsumerProps = {
    val props = new Properties()
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, DefaultBootstrapServers)
    props.put(ConsumerConfig.GROUP_ID_CONFIG, randomName(DefaultNameLeght))
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, SerdeName)
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, SerdeName)
    props
  }

  private lazy val StreamApplicationId = randomName(DefaultNameLeght)

  private lazy val StreamProps = {
    val props = new Properties()
    props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, DefaultBootstrapServers)
    props.put(StreamsConfig.APPLICATION_ID_CONFIG, StreamApplicationId)
    props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, SerdeName)
    props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, SerdeName)
    props.put(StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG, TimestampExtractorName)
    props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, DefaultCommitInterval.toMillis.asInstanceOf[AnyRef])
    props
  }

  implicit class KafkaProducerOps[K, V](val producer: KafkaProducer[K, V]) {
    def send(topic: String, key: K, value: V): Unit =
      producer.send(new ProducerRecord(topic, key, value))
  }

}

trait Kafka extends LazyLogging {

  import Kafka._

  def kafkaStart(): Unit = {
    logger.info("Starting embedded Kafka")

    implicit val config = EmbeddedKafkaConfig(DefaultKafkaPort, DefaultZkPort)
    EmbeddedKafka.start()

    logger.info(s"Embedded Kafka started on $DefaultKafkaPort")
  }

  def kafkaStop(): Unit = {
    logger.info(s"Stopping embedded Kafka on $DefaultKafkaPort")

    EmbeddedKafka.stop()

    logger.info("Embedded Kafka stopped")

  }

  def startStreams(topology: Topology): Unit = {
    logger.info("Starting stream")

    val streams = new KafkaStreams(topology, StreamProps)
    streams.cleanUp()
    streams.start()

    logger.info(s"Stream started '$StreamApplicationId'")
  }

  def kafkaProduce(block: GenericProducer => Unit): Unit = {
    val producer = new KafkaProducer[AnyRef, AnyRef](ProducerProps)
    block(producer)
  }

  def kafkaConsume(topicName: String)(block: GenericConsumerRecord => Unit): Unit = {
    import scala.collection.JavaConverters._

    val consumer = new KafkaConsumer[AnyRef, AnyRef](ConsumerProps)
    consumer.subscribe(Seq(topicName).asJava)

    while (true) {
      val recs = consumer.poll(DefaultPollTime.toMillis)
      recs.iterator().asScala.foreach { record =>
        block(record)
      }
    }
  }

}
