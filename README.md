# Kafka Streams DSL vs Processor API

Features
========

* Almost identical not so trivial topologies implemented using DSL and Processor API, see 
[https://github.com/mkuthan/example-kafkastreams/blob/master/src/main/scala/example/ClickstreamJoinExample.scala](https://github.com/mkuthan/example-kafkastreams/blob/master/src/main/scala/example/ClickstreamJoinExample.scala)
* Processor API version is 10 times more efficient than DSL version.
* Examples are configured with 
[Embedded Kafka](https://github.com/manub/scalatest-embedded-kafka)
and does not require any additional setup.
* Implemented with Kafka 1.0

References
==========

* [http://mkuthan.github.io/blog/2017/11/02/kafka-streams-dsl-vs-processor-api/](http://mkuthan.github.io/blog/2017/11/02/kafka-streams-dsl-vs-processor-api/)
