package example

import java.util.Properties

import com.typesafe.scalalogging.LazyLogging
import net.manub.embeddedkafka.{EmbeddedKafka, EmbeddedKafkaConfig}
import org.apache.kafka.clients.consumer.{ConsumerConfig, KafkaConsumer}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.serialization.{Serdes, StringDeserializer, StringSerializer}
import org.apache.kafka.streams.kstream.{JoinWindows, KStreamBuilder, ValueJoiner}
import org.apache.kafka.streams.processor.FailOnInvalidTimestamp
import org.apache.kafka.streams.{KafkaStreams, StreamsConfig}

import scala.collection.JavaConverters._
import scala.io.StdIn

object LeftJoinExample extends LazyLogging {

  def main(args: Array[String]): Unit = {
    implicit val config = EmbeddedKafkaConfig(9092, 2181)
    EmbeddedKafka.start()

    daemonThread {
      produce()
    }

    daemonThread {
      join()
    }

    daemonThread {
      printResults()
    }

    StdIn.readLine()

    EmbeddedKafka.stop()
  }

  def produce(): Unit = {
    val producer = new KafkaProducer[K, V](producerProps)

    for (i <- 0 to 999) {
      val key = "%03d".format(i)
      producer.send(new ProducerRecord("this", key, "this"))
      for (j <- 0 to 5) {
        Thread.sleep(200L)
        producer.send(new ProducerRecord("other", key, s"$j"))
      }
      Thread.sleep(5000L)
    }

    producer.close()
  }

  def join(): Unit = {
    Thread.sleep(5000L)

    val builder = new KStreamBuilder()

    val thisStream = builder.stream[K, V]("this")
    val otherStream = builder.stream[K, V]("other")

    val joinedStream = thisStream.join(
      otherStream,
      valueJoiner { (t: String, o: String) =>
        t + o
      },
      JoinWindows.of(10000L).until(10 * 10000L)
    )

    joinedStream.to("results")

    val streams = new KafkaStreams(builder, streamProps)
    streams.start()
  }

  def printResults(): Unit = {
    val consumer = new KafkaConsumer[K, V](consumerProps)
    consumer.subscribe(Seq("results").asJava)

    while (true) {
      val recs = consumer.poll(60000L)
      val recIt = recs.iterator()
      while (recIt.hasNext) {
        val record = recIt.next()
        logger.info(s"${record.key}: ${record.value}")
      }
    }
  }

  def daemonThread(block: => Unit): Unit = {
    val t = new Thread(() => block)
    t.setDaemon(true)
    t.start()
  }

  def valueJoiner[Value1, Value2, Out](fn: (Value1, Value2) => Out) = new ValueJoiner[Value1, Value2, Out] {
    override def apply(value1: Value1, value2: Value2): Out = fn(value1, value2)
  }

  lazy val producerProps = {
    val props = new Properties()
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)
    props
  }

  lazy val consumerProps = {
    val props = new Properties()
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
    props.put(ConsumerConfig.GROUP_ID_CONFIG, "left-join-example-consumer")
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer].getName)
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer].getName)
    props
  }

  def streamProps = {
    val props = new Properties()
    props.put(StreamsConfig.APPLICATION_ID_CONFIG, "left-join-example-stream")
    props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
    props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass.getName)
    props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass.getName)
    props.put(StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG, classOf[FailOnInvalidTimestamp].getName)
    props
  }

  type K = String
  type V = String
}
