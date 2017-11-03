# Kafka Streams DSL vs Processor API

[![Build Status](https://travis-ci.org/mkuthan/example-kafkastreams.svg?branch=master)](https://travis-ci.org/mkuthan/example-kafkastreams)

Features
========

* Warm-up Processor API exercise, see 
[DeduplicationExample](https://github.com/mkuthan/example-kafkastreams/blob/master/src/main/scala/example/DeduplicationExample.scala).
* Clickstream join topology implemented using DSL and Processor API, see 
[ClickstreamJoinExample](https://github.com/mkuthan/example-kafkastreams/blob/master/src/main/scala/example/ClickstreamJoinExample.scala).
* Processor API version is up to 10 times more efficient than DSL version.
* Examples are configured with 
[Embedded Kafka](https://github.com/manub/scalatest-embedded-kafka)
and does not require any additional setup.
* Implemented with Kafka 1.0

Missing Features
========
* Tests

References
==========

* [http://mkuthan.github.io/blog/2017/11/02/kafka-streams-dsl-vs-processor-api/](http://mkuthan.github.io/blog/2017/11/02/kafka-streams-dsl-vs-processor-api/)
