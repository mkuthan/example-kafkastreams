package example

import java.util.Properties

import com.typesafe.scalalogging.LazyLogging
import net.manub.embeddedkafka.{EmbeddedKafka, EmbeddedKafkaConfig}
import org.apache.kafka.clients.consumer.{ConsumerConfig, KafkaConsumer}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.serialization.{Serdes, StringDeserializer, StringSerializer}
import org.apache.kafka.streams.kstream.{JoinWindows, KStream, KStreamBuilder, Transformer, TransformerSupplier, ValueJoiner}
import org.apache.kafka.streams.processor.{FailOnInvalidTimestamp, ProcessorContext}
import org.apache.kafka.streams.state.{KeyValueStore, Stores}
import org.apache.kafka.streams.{KafkaStreams, KeyValue, StreamsConfig}

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
      producer.send(new ProducerRecord("this", key, key))
      for (j <- 0 to 5) {
        Thread.sleep(200L)
        producer.send(new ProducerRecord("other", key, s"$j"))
      }
      Thread.sleep(5000L)
    }

    producer.close()
  }

  def join(): Unit = {
    val AggregationStoreName = "aggregation-store"

    Thread.sleep(5000L)

    val builder = new KStreamBuilder()

    val thisStream: KStream[K, V] = builder.stream[K, V]("this")
    val otherStream: KStream[K, V] = builder.stream[K, V]("other")

    val joinedStream: KStream[K, V] = thisStream.join(
      otherStream,
      valueJoiner { (t: String, o: String) =>
        s"$t->$o"
      },
      JoinWindows.of(10000L).until(10 * 10000L)
    )

    val aggregator: Transformer[K, V, KeyValue[K, V]] = new Transformer[K, V, KeyValue[K, V]]() {
      var context: ProcessorContext = _
      var store: KeyValueStore[K, V] = _

      override def init(ctx: ProcessorContext): Unit = {
        context = ctx

        context.schedule(10000L)
        store = context.getStateStore(AggregationStoreName).asInstanceOf[KeyValueStore[K, V]]
      }

      override def transform(key: K, value: V): KeyValue[K, V] = {
        val oldValue = store.get(key)
        if (oldValue == null) {
          store.put(key, value)
        } else {
          store.put(key, s"$oldValue, $value")
        }
        null
      }

      override def punctuate(timestamp: Long): KeyValue[K, V] = {
        val it = store.all()
        while (it.hasNext) {
          val kv = it.next()
          context.forward(kv.key, kv.value)
          store.delete(kv.key)
        }
        it.close()

        context.commit()
        null
      }

      override def close() = {}
    }

    builder.addStateStore(
      Stores.create(AggregationStoreName)
          .withKeys(classOf[K])
          .withValues(classOf[V])
          .persistent()
          .build()
    )

    val aggregatedStream: KStream[K, V] = joinedStream.transform(
      transformerSupplier {
        aggregator
      },
      AggregationStoreName
    )

    aggregatedStream.to("results")

    val streams = new KafkaStreams(builder, streamProps)
    streams.cleanUp()
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

  def valueJoiner[V1, V2, Out](f: (V1, V2) => Out) = new ValueJoiner[V1, V2, Out] {
    override def apply(v1: V1, v2: V2): Out = f(v1, v2)
  }

  def transformerSupplier[K1, V1, Out](f: => Transformer[K1, V1, Out]) = new TransformerSupplier[K1, V1, Out] {
    override def get(): Transformer[K1, V1, Out] = f
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

