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

import org.apache.kafka.streams.KeyValue
import org.apache.kafka.streams.kstream.Transformer
import org.apache.kafka.streams.processor.ProcessorContext

/*
 * Encapsulates Java centric Kafka Stream transformer.
 */
trait AbstractTransformer extends Transformer[K, V, KeyValue[K, V]] {
  private var _context: ProcessorContext = _

  def context: ProcessorContext = _context

  final override def init(context: ProcessorContext): Unit = {
    _context = context
    doInit()
  }

  final override def transform(key: K, value: V): KeyValue[K, V] = {
    doTransform(key, value)
    null
  }

  final override def punctuate(timestamp: Long): KeyValue[K, V] = {
    doPunctuate(timestamp)
    null
  }

  final override def close(): Unit =
    doClose()

  def doInit(): Unit = {}

  def doTransform(key: K, value: V): Unit

  def doPunctuate(timestamp: Long): Unit = {}

  def doClose(): Unit = {}
}
