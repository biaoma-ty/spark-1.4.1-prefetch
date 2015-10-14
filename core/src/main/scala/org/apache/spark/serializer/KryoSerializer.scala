/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.serializer

import java.io.{EOFException, IOException, InputStream, OutputStream}
import java.nio.ByteBuffer
import javax.annotation.Nullable

import scala.reflect.ClassTag

import com.esotericsoftware.kryo.{Kryo, KryoException}
import com.esotericsoftware.kryo.io.{Input => KryoInput, Output => KryoOutput}
import com.esotericsoftware.kryo.serializers.{JavaSerializer => KryoJavaSerializer}
import com.twitter.chill.{AllScalaRegistrar, EmptyScalaKryoInstantiator}
import org.roaringbitmap.{ArrayContainer, BitmapContainer, RoaringArray, RoaringBitmap}

import org.apache.spark._
import org.apache.spark.api.python.PythonBroadcast
import org.apache.spark.broadcast.HttpBroadcast
import org.apache.spark.network.nio.{GetBlock, GotBlock, PutBlock}
import org.apache.spark.network.util.ByteUnit
import org.apache.spark.scheduler.{CompressedMapStatus, HighlyCompressedMapStatus}
import org.apache.spark.storage._
import org.apache.spark.util.BoundedPriorityQueue
import org.apache.spark.util.collection.CompactBuffer

/**
 * A Spark serializer that uses the [[https://code.google.com/p/kryo/ Kryo serialization library]].
 *
 * Note that this serializer is not guaranteed to be wire-compatible across different versions of
 * Spark. It is intended to be used to serialize/de-serialize data within a single
 * Spark application.
 */
class KryoSerializer(conf: SparkConf)
  extends org.apache.spark.serializer.Serializer
  with Logging
  with Serializable {

  private val bufferSizeKb = conf.getSizeAsKb("spark.kryoserializer.buffer", "64k")

  if (bufferSizeKb >= ByteUnit.GiB.toKiB(2)) {
    throw new IllegalArgumentException("spark.kryoserializer.buffer must be less than " +
      s"2048 mb, got: + ${ByteUnit.KiB.toMiB(bufferSizeKb)} mb.")
  }
  private val bufferSize = ByteUnit.KiB.toBytes(bufferSizeKb).toInt

  val maxBufferSizeMb = conf.getSizeAsMb("spark.kryoserializer.buffer.max", "64m").toInt
  if (maxBufferSizeMb >= ByteUnit.GiB.toMiB(2)) {
    throw new IllegalArgumentException("spark.kryoserializer.buffer.max must be less than " +
      s"2048 mb, got: + $maxBufferSizeMb mb.")
  }
  private val maxBufferSize = ByteUnit.MiB.toBytes(maxBufferSizeMb).toInt

  private val referenceTracking = conf.getBoolean("spark.kryo.referenceTracking", true)
  private val registrationRequired = conf.getBoolean("spark.kryo.registrationRequired", false)
  private val userRegistrator = conf.getOption("spark.kryo.registrator")
  private val classesToRegister = conf.get("spark.kryo.classesToRegister", "")
    .split(',')
    .filter(!_.isEmpty)

  def newKryoOutput(): KryoOutput = new KryoOutput(bufferSize, math.max(bufferSize, maxBufferSize))

  def newKryo(): Kryo = {
    val instantiator = new EmptyScalaKryoInstantiator
    val kryo = instantiator.newKryo()
    kryo.setRegistrationRequired(registrationRequired)

    val oldClassLoader = Thread.currentThread.getContextClassLoader
    val classLoader = defaultClassLoader.getOrElse(Thread.currentThread.getContextClassLoader)

    // Allow disabling Kryo reference tracking if user knows their object graphs don't have loops.
    // Do this before we invoke the user registrator so the user registrator can override this.
    kryo.setReferences(referenceTracking)

    for (cls <- KryoSerializer.toRegister) {
      kryo.register(cls)
    }

    // For results returned by asJavaIterable. See JavaIterableWrapperSerializer.
    kryo.register(JavaIterableWrapperSerializer.wrapperClass, new JavaIterableWrapperSerializer)

    // Allow sending SerializableWritable
    kryo.register(classOf[SerializableWritable[_]], new KryoJavaSerializer())
    kryo.register(classOf[HttpBroadcast[_]], new KryoJavaSerializer())
    kryo.register(classOf[PythonBroadcast], new KryoJavaSerializer())

    try {
      // Use the default classloader when calling the user registrator.
      Thread.currentThread.setContextClassLoader(classLoader)
      // Register classes given through spark.kryo.classesToRegister.
      classesToRegister
        .foreach { className => kryo.register(Class.forName(className, true, classLoader)) }
      // Allow the user to register their own classes by setting spark.kryo.registrator.
      userRegistrator
        .map(Class.forName(_, true, classLoader).newInstance().asInstanceOf[KryoRegistrator])
        .foreach { reg => reg.registerClasses(kryo) }
    } catch {
      case e: Exception =>
        throw new SparkException(s"Failed to register classes with Kryo", e)
    } finally {
      Thread.currentThread.setContextClassLoader(oldClassLoader)
    }

    // Register Chill's classes; we do this after our ranges and the user's own classes to let
    // our code override the generic serializers in Chill for things like Seq
    new AllScalaRegistrar().apply(kryo)

    kryo.setClassLoader(classLoader)
    kryo
  }

  override def newInstance(): SerializerInstance = {
    new KryoSerializerInstance(this)
  }

  private[spark] override lazy val supportsRelocationOfSerializedObjects: Boolean = {
    // If auto-reset is disabled, then Kryo may store references to duplicate occurrences of objects
    // in the stream rather than writing those objects' serialized bytes, breaking relocation. See
    // https://groups.google.com/d/msg/kryo-users/6ZUSyfjjtdo/FhGG1KHDXPgJ for more details.
    newInstance().asInstanceOf[KryoSerializerInstance].getAutoReset()
  }
}

private[spark]
class KryoSerializationStream(
    serInstance: KryoSerializerInstance,
    outStream: OutputStream) extends SerializationStream {

  private[this] var output: KryoOutput = new KryoOutput(outStream)
  private[this] var kryo: Kryo = serInstance.borrowKryo()

  override def writeObject[T: ClassTag](t: T): SerializationStream = {
    kryo.writeClassAndObject(output, t)
    this
  }

  override def flush() {
    if (output == null) {
      throw new IOException("Stream is closed")
    }
    output.flush()
  }

  override def close() {
    if (output != null) {
      try {
        output.close()
      } finally {
        serInstance.releaseKryo(kryo)
        kryo = null
        output = null
      }
    }
  }
}

private[spark]
class KryoDeserializationStream(
    serInstance: KryoSerializerInstance,
    inStream: InputStream) extends DeserializationStream {

  private[this] var input: KryoInput = new KryoInput(inStream)
  private[this] var kryo: Kryo = serInstance.borrowKryo()

  override def readObject[T: ClassTag](): T = {
    try {
      kryo.readClassAndObject(input).asInstanceOf[T]
    } catch {
      // DeserializationStream uses the EOF exception to indicate stopping condition.
      case e: KryoException if e.getMessage.toLowerCase.contains("buffer underflow") =>
        throw new EOFException
    }
  }

  override def close() {
    if (input != null) {
      try {
        // Kryo's Input automatically closes the input stream it is using.
        input.close()
      } finally {
        serInstance.releaseKryo(kryo)
        kryo = null
        input = null
      }
    }
  }
}

private[spark] class KryoSerializerInstance(ks: KryoSerializer) extends SerializerInstance {

  /**
   * A re-used [[Kryo]] instance. Methods will borrow this instance by calling `borrowKryo()`, do
   * their work, then release the instance by calling `releaseKryo()`. Logically, this is a caching
   * pool of size one. SerializerInstances are not thread-safe, hence accesses to this field are
   * not synchronized.
   */
  @Nullable private[this] var cachedKryo: Kryo = borrowKryo()

  /**
   * Borrows a [[Kryo]] instance. If possible, this tries to re-use a cached Kryo instance;
   * otherwise, it allocates a new instance.
   */
  private[serializer] def borrowKryo(): Kryo = {
    if (cachedKryo != null) {
      val kryo = cachedKryo
      // As a defensive measure, call reset() to clear any Kryo state that might have been modified
      // by the last operation to borrow this instance (see SPARK-7766 for discussion of this issue)
      kryo.reset()
      cachedKryo = null
      kryo
    } else {
      ks.newKryo()
    }
  }

  /**
   * Release a borrowed [[Kryo]] instance. If this serializer instance already has a cached Kryo
   * instance, then the given Kryo instance is discarded; otherwise, the Kryo is stored for later
   * re-use.
   */
  private[serializer] def releaseKryo(kryo: Kryo): Unit = {
    if (cachedKryo == null) {
      cachedKryo = kryo
    }
  }

  // Make these lazy vals to avoid creating a buffer unless we use them.
  private lazy val output = ks.newKryoOutput()
  private lazy val input = new KryoInput()

  override def serialize[T: ClassTag](t: T): ByteBuffer = {
    output.clear()
    val kryo = borrowKryo()
    try {
      kryo.writeClassAndObject(output, t)
    } catch {
      case e: KryoException if e.getMessage.startsWith("Buffer overflow") =>
        throw new SparkException(s"Kryo serialization failed: ${e.getMessage}. To avoid this, " +
          "increase spark.kryoserializer.buffer.max value.")
    } finally {
      releaseKryo(kryo)
    }
    ByteBuffer.wrap(output.toBytes)
  }

  override def deserialize[T: ClassTag](bytes: ByteBuffer): T = {
    val kryo = borrowKryo()
    try {
      input.setBuffer(bytes.array)
      kryo.readClassAndObject(input).asInstanceOf[T]
    } finally {
      releaseKryo(kryo)
    }
  }

  override def deserialize[T: ClassTag](bytes: ByteBuffer, loader: ClassLoader): T = {
    val kryo = borrowKryo()
    val oldClassLoader = kryo.getClassLoader
    try {
      kryo.setClassLoader(loader)
      input.setBuffer(bytes.array)
      kryo.readClassAndObject(input).asInstanceOf[T]
    } finally {
      kryo.setClassLoader(oldClassLoader)
      releaseKryo(kryo)
    }
  }

  override def serializeStream(s: OutputStream): SerializationStream = {
    new KryoSerializationStream(this, s)
  }

  override def deserializeStream(s: InputStream): DeserializationStream = {
    new KryoDeserializationStream(this, s)
  }

  /**
   * Returns true if auto-reset is on. The only reason this would be false is if the user-supplied
   * registrator explicitly turns auto-reset off.
   */
  def getAutoReset(): Boolean = {
    val field = classOf[Kryo].getDeclaredField("autoReset")
    field.setAccessible(true)
    val kryo = borrowKryo()
    try {
      field.get(kryo).asInstanceOf[Boolean]
    } finally {
      releaseKryo(kryo)
    }
  }
}

/**
 * Interface implemented by clients to register their classes with Kryo when using Kryo
 * serialization.
 */
trait KryoRegistrator {
  def registerClasses(kryo: Kryo)
}

private[serializer] object KryoSerializer {
  // Commonly used classes.
  private val toRegister: Seq[Class[_]] = Seq(
    ByteBuffer.allocate(1).getClass,
    classOf[StorageLevel],
    classOf[PutBlock],
    classOf[GotBlock],
    classOf[GetBlock],
    classOf[CompressedMapStatus],
    classOf[HighlyCompressedMapStatus],
    classOf[RoaringBitmap],
    classOf[RoaringArray],
    classOf[RoaringArray.Element],
    classOf[Array[RoaringArray.Element]],
    classOf[ArrayContainer],
    classOf[BitmapContainer],
    classOf[CompactBuffer[_]],
    classOf[BlockManagerId],
    classOf[Array[Byte]],
    classOf[Array[Short]],
    classOf[Array[Long]],
    classOf[BoundedPriorityQueue[_]],
    classOf[SparkConf]
  )
}

/**
 * A Kryo serializer for serializing results returned by asJavaIterable.
 *
 * The underlying object is scala.collection.convert.Wrappers$IterableWrapper.
 * Kryo deserializes this into an AbstractCollection, which unfortunately doesn't work.
 */
private class JavaIterableWrapperSerializer
  extends com.esotericsoftware.kryo.Serializer[java.lang.Iterable[_]] {

  import JavaIterableWrapperSerializer._

  override def write(kryo: Kryo, out: KryoOutput, obj: java.lang.Iterable[_]): Unit = {
    // If the object is the wrapper, simply serialize the underlying Scala Iterable object.
    // Otherwise, serialize the object itself.
    if (obj.getClass == wrapperClass && underlyingMethodOpt.isDefined) {
      kryo.writeClassAndObject(out, underlyingMethodOpt.get.invoke(obj))
    } else {
      kryo.writeClassAndObject(out, obj)
    }
  }

  override def read(kryo: Kryo, in: KryoInput, clz: Class[java.lang.Iterable[_]])
    : java.lang.Iterable[_] = {
    kryo.readClassAndObject(in) match {
      case scalaIterable: Iterable[_] =>
        scala.collection.JavaConversions.asJavaIterable(scalaIterable)
      case javaIterable: java.lang.Iterable[_] =>
        javaIterable
    }
  }
}

private object JavaIterableWrapperSerializer extends Logging {
  // The class returned by asJavaIterable (scala.collection.convert.Wrappers$IterableWrapper).
  val wrapperClass =
    scala.collection.convert.WrapAsJava.asJavaIterable(Seq(1)).getClass

  // Get the underlying method so we can use it to get the Scala collection for serialization.
  private val underlyingMethodOpt = {
    try Some(wrapperClass.getDeclaredMethod("underlying")) catch {
      case e: Exception =>
        logError("Failed to find the underlying field in " + wrapperClass, e)
        None
    }
  }
}
