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

import com.twitter.chill.KryoPool
import com.twitter.chill.ScalaKryoInstantiator
import org.apache.kafka.common.serialization.Deserializer
import org.apache.kafka.common.serialization.Serde
import org.apache.kafka.common.serialization.Serializer

class KryoSerde[T <: AnyRef] extends Deserializer[T] with Serializer[T] with Serde[T] {

  import KryoSerde._

  lazy val kryo = KryoPool.withBuffer(
    DefaultPoolSize,
    new ScalaKryoInstantiator(),
    OutputBufferInitial,
    OutputBufferMax
  )

  override def deserialize(topic: String, data: Array[Byte]): T =
    kryo.fromBytes(data).asInstanceOf[T]

  override def serialize(topic: String, data: T): Array[Byte] =
    kryo.toBytesWithClass(data)

  override def deserializer(): Deserializer[T] = this

  override def serializer(): Serializer[T] = this

  override def configure(configs: java.util.Map[String, _], isKey: Boolean): Unit = {}

  override def close(): Unit = {}

}

object KryoSerde {

  private val DefaultPoolSize = 10
  private val OutputBufferInitial = 1024
  private val OutputBufferMax = 10 * 1024 * 1024

  def apply[T <: AnyRef](): KryoSerde[T] = new KryoSerde[T]()

}
