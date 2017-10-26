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
import org.apache.kafka.common.serialization.{Serdes, StringDeserializer, StringSerializer}
import org.apache.kafka.streams.{KafkaStreams, KeyValue, StreamsConfig, Topology}
import org.apache.kafka.streams.kstream.{JoinWindows, KStream, Transformer, TransformerSupplier, ValueJoiner}
import org.apache.kafka.streams.processor.FailOnInvalidTimestamp

object Kafka {

  private lazy val producerProps = {
    val props = new Properties()
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)
    props
  }

  private lazy val consumerProps = {
    val props = new Properties()
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
    props.put(ConsumerConfig.GROUP_ID_CONFIG, "left-join-example-consumer")
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer].getName)
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer].getName)
    props
  }

  private lazy val streamProps = {
    val props = new Properties()
    props.put(StreamsConfig.APPLICATION_ID_CONFIG, "left-join-example-stream")
    props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
    props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass.getName)
    props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass.getName)
    props.put(StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG, classOf[FailOnInvalidTimestamp].getName)
    props
  }

  implicit class KafkaProducerOps(val producer: KafkaProducer[K, V]) {
    def send(topic: String, key: K, value: V): Unit =
      producer.send(new ProducerRecord(topic, key, value))
  }

  implicit class KStreamOps(val kStream: KStream[K, V]) {
    def sjoin(other: KStream[K, V], windows: JoinWindows)(f: (V, V) => V): KStream[K, V] =
      kStream.join(
        other,
        new ValueJoiner[V, V, V] {
          override def apply(v1: V, v2: V) = f(v1, v2)
        },
        windows
      )

    def stransform(storeName: String)(f: => Transformer[K, V, KeyValue[K, V]]): KStream[K, V] =
      kStream.transform(
        new TransformerSupplier[K, V, KeyValue[K, V]] {
          override def get() = f
        },
        storeName
      )
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

  def kafkaProduce(block: KafkaProducer[K, V] => Unit): Unit = {
    val producer = new KafkaProducer[K, V](producerProps)
    block(producer)
  }

  def kafkaConsume(topicName: String)(block: ConsumerRecord[K, V] => Unit): Unit = {
    import scala.collection.JavaConverters._

    val consumer = new KafkaConsumer[K, V](consumerProps)
    consumer.subscribe(Seq(topicName).asJava)

    while (true) {
      val recs = consumer.poll(60000L)
      val recIt = recs.iterator()
      while (recIt.hasNext) {
        block(recIt.next())
      }
    }
  }

  def startStreams(topology: Topology): Unit = {
    val streams = new KafkaStreams(topology, streamProps)
    streams.cleanUp()
    streams.start()
  }

}
