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

package org.apache.spark.streaming

import java.io.{File, NotSerializableException}
import java.util.concurrent.atomic.AtomicInteger

import scala.collection.mutable.Queue

import org.apache.commons.io.FileUtils
import org.scalatest.concurrent.Eventually._
import org.scalatest.concurrent.Timeouts
import org.scalatest.exceptions.TestFailedDueToTimeoutException
import org.scalatest.time.SpanSugar._
import org.scalatest.{Assertions, BeforeAndAfter}

import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.receiver.Receiver
import org.apache.spark.util.Utils
import org.apache.spark.{Logging, SparkConf, SparkContext, SparkException, SparkFunSuite}


class StreamingContextSuite extends SparkFunSuite with BeforeAndAfter with Timeouts with Logging {

  val master = "local[2]"
  val appName = this.getClass.getSimpleName
  val batchDuration = Milliseconds(500)
  val sparkHome = "someDir"
  val envPair = "key" -> "value"
  val conf = new SparkConf().setMaster(master).setAppName(appName)

  var sc: SparkContext = null
  var ssc: StreamingContext = null

  after {
    if (ssc != null) {
      ssc.stop()
      ssc = null
    }
    if (sc != null) {
      sc.stop()
      sc = null
    }
  }

  test("from no conf constructor") {
    ssc = new StreamingContext(master, appName, batchDuration)
    assert(ssc.sparkContext.conf.get("spark.master") === master)
    assert(ssc.sparkContext.conf.get("spark.app.name") === appName)
  }

  test("from no conf + spark home") {
    ssc = new StreamingContext(master, appName, batchDuration, sparkHome, Nil)
    assert(ssc.conf.get("spark.home") === sparkHome)
  }

  test("from no conf + spark home + env") {
    ssc = new StreamingContext(master, appName, batchDuration,
      sparkHome, Nil, Map(envPair))
    assert(ssc.conf.getExecutorEnv.contains(envPair))
  }

  test("from conf with settings") {
    val myConf = SparkContext.updatedConf(new SparkConf(false), master, appName)
    myConf.set("spark.cleaner.ttl", "10s")
    ssc = new StreamingContext(myConf, batchDuration)
    assert(ssc.conf.getTimeAsSeconds("spark.cleaner.ttl", "-1") === 10)
  }

  test("from existing SparkContext") {
    sc = new SparkContext(master, appName)
    ssc = new StreamingContext(sc, batchDuration)
  }

  test("from existing SparkContext with settings") {
    val myConf = SparkContext.updatedConf(new SparkConf(false), master, appName)
    myConf.set("spark.cleaner.ttl", "10s")
    ssc = new StreamingContext(myConf, batchDuration)
    assert(ssc.conf.getTimeAsSeconds("spark.cleaner.ttl", "-1") === 10)
  }

  test("from checkpoint") {
    val myConf = SparkContext.updatedConf(new SparkConf(false), master, appName)
    myConf.set("spark.cleaner.ttl", "10s")
    val ssc1 = new StreamingContext(myConf, batchDuration)
    addInputStream(ssc1).register()
    ssc1.start()
    val cp = new Checkpoint(ssc1, Time(1000))
    assert(
      Utils.timeStringAsSeconds(cp.sparkConfPairs
          .toMap.getOrElse("spark.cleaner.ttl", "-1")) === 10)
    ssc1.stop()
    val newCp = Utils.deserialize[Checkpoint](Utils.serialize(cp))
    assert(newCp.createSparkConf().getTimeAsSeconds("spark.cleaner.ttl", "-1") === 10)
    ssc = new StreamingContext(null, newCp, null)
    assert(ssc.conf.getTimeAsSeconds("spark.cleaner.ttl", "-1") === 10)
  }

  test("state matching") {
    import StreamingContextState._
    assert(INITIALIZED === INITIALIZED)
    assert(INITIALIZED != ACTIVE)
  }

  test("start and stop state check") {
    ssc = new StreamingContext(master, appName, batchDuration)
    addInputStream(ssc).register()

    assert(ssc.getState() === StreamingContextState.INITIALIZED)
    ssc.start()
    assert(ssc.getState() === StreamingContextState.ACTIVE)
    ssc.stop()
    assert(ssc.getState() === StreamingContextState.STOPPED)

    // Make sure that the SparkContext is also stopped by default
    intercept[Exception] {
      ssc.sparkContext.makeRDD(1 to 10)
    }
  }

  test("start with non-seriazable DStream checkpoints") {
    val checkpointDir = Utils.createTempDir()
    ssc = new StreamingContext(conf, batchDuration)
    ssc.checkpoint(checkpointDir.getAbsolutePath)
    addInputStream(ssc).foreachRDD { rdd =>
      // Refer to this.appName from inside closure so that this closure refers to
      // the instance of StreamingContextSuite, and is therefore not serializable
      rdd.count() + appName
    }

    // Test whether start() fails early when checkpointing is enabled
    val exception = intercept[NotSerializableException] {
      ssc.start()
    }
    assert(exception.getMessage().contains("DStreams with their functions are not serializable"))
    assert(ssc.getState() !== StreamingContextState.ACTIVE)
    assert(StreamingContext.getActive().isEmpty)
  }

  test("start failure should stop internal components") {
    ssc = new StreamingContext(conf, batchDuration)
    val inputStream = addInputStream(ssc)
    val updateFunc = (values: Seq[Int], state: Option[Int]) => {
      Some(values.sum + state.getOrElse(0))
    }
    inputStream.map(x => (x, 1)).updateStateByKey[Int](updateFunc)
    // Require that the start fails because checkpoint directory was not set
    intercept[Exception] {
      ssc.start()
    }
    assert(ssc.getState() === StreamingContextState.STOPPED)
    assert(ssc.scheduler.isStarted === false)
  }


  test("start multiple times") {
    ssc = new StreamingContext(master, appName, batchDuration)
    addInputStream(ssc).register()
    ssc.start()
    assert(ssc.getState() === StreamingContextState.ACTIVE)
    ssc.start()
    assert(ssc.getState() === StreamingContextState.ACTIVE)
  }

  test("stop multiple times") {
    ssc = new StreamingContext(master, appName, batchDuration)
    addInputStream(ssc).register()
    ssc.start()
    ssc.stop()
    assert(ssc.getState() === StreamingContextState.STOPPED)
    ssc.stop()
    assert(ssc.getState() === StreamingContextState.STOPPED)
  }

  test("stop before start") {
    ssc = new StreamingContext(master, appName, batchDuration)
    addInputStream(ssc).register()
    ssc.stop()  // stop before start should not throw exception
    assert(ssc.getState() === StreamingContextState.STOPPED)
  }

  test("start after stop") {
    // Regression test for SPARK-4301
    ssc = new StreamingContext(master, appName, batchDuration)
    addInputStream(ssc).register()
    ssc.stop()
    intercept[IllegalStateException] {
      ssc.start() // start after stop should throw exception
    }
    assert(ssc.getState() === StreamingContextState.STOPPED)
  }

  test("stop only streaming context") {
    val conf = new SparkConf().setMaster(master).setAppName(appName)

    // Explicitly do not stop SparkContext
    ssc = new StreamingContext(conf, batchDuration)
    sc = ssc.sparkContext
    addInputStream(ssc).register()
    ssc.start()
    ssc.stop(stopSparkContext = false)
    assert(ssc.getState() === StreamingContextState.STOPPED)
    assert(sc.makeRDD(1 to 100).collect().size === 100)
    sc.stop()

    // Implicitly do not stop SparkContext
    conf.set("spark.streaming.stopSparkContextByDefault", "false")
    ssc = new StreamingContext(conf, batchDuration)
    sc = ssc.sparkContext
    addInputStream(ssc).register()
    ssc.start()
    ssc.stop()
    assert(sc.makeRDD(1 to 100).collect().size === 100)
    sc.stop()
  }

  test("stop(stopSparkContext=true) after stop(stopSparkContext=false)") {
    ssc = new StreamingContext(master, appName, batchDuration)
    addInputStream(ssc).register()
    ssc.stop(stopSparkContext = false)
    assert(ssc.sc.makeRDD(1 to 100).collect().size === 100)
    ssc.stop(stopSparkContext = true)
    // Check that the SparkContext is actually stopped:
    intercept[Exception] {
      ssc.sc.makeRDD(1 to 100).collect()
    }
  }

  test("stop gracefully") {
    val conf = new SparkConf().setMaster(master).setAppName(appName)
    conf.set("spark.cleaner.ttl", "3600s")
    sc = new SparkContext(conf)
    for (i <- 1 to 4) {
      logInfo("==================================\n\n\n")
      ssc = new StreamingContext(sc, Milliseconds(100))
      var runningCount = 0
      TestReceiver.counter.set(1)
      val input = ssc.receiverStream(new TestReceiver)
      input.count().foreachRDD { rdd =>
        val count = rdd.first()
        runningCount += count.toInt
        logInfo("Count = " + count + ", Running count = " + runningCount)
      }
      ssc.start()
      ssc.awaitTerminationOrTimeout(500)
      ssc.stop(stopSparkContext = false, stopGracefully = true)
      logInfo("Running count = " + runningCount)
      logInfo("TestReceiver.counter = " + TestReceiver.counter.get())
      assert(runningCount > 0)
      assert(
        (TestReceiver.counter.get() == runningCount + 1) ||
          (TestReceiver.counter.get() == runningCount + 2),
        "Received records = " + TestReceiver.counter.get() + ", " +
          "processed records = " + runningCount
      )
      Thread.sleep(100)
    }
  }

  test("stop slow receiver gracefully") {
    val conf = new SparkConf().setMaster(master).setAppName(appName)
    conf.set("spark.streaming.gracefulStopTimeout", "20000s")
    sc = new SparkContext(conf)
    logInfo("==================================\n\n\n")
    ssc = new StreamingContext(sc, Milliseconds(100))
    var runningCount = 0
    SlowTestReceiver.receivedAllRecords = false
    // Create test receiver that sleeps in onStop()
    val totalNumRecords = 15
    val recordsPerSecond = 1
    val input = ssc.receiverStream(new SlowTestReceiver(totalNumRecords, recordsPerSecond))
    input.count().foreachRDD { rdd =>
      val count = rdd.first()
      runningCount += count.toInt
      logInfo("Count = " + count + ", Running count = " + runningCount)
    }
    ssc.start()
    ssc.awaitTerminationOrTimeout(500)
    ssc.stop(stopSparkContext = false, stopGracefully = true)
    logInfo("Running count = " + runningCount)
    assert(runningCount > 0)
    assert(runningCount == totalNumRecords)
    Thread.sleep(100)
  }

  test("awaitTermination") {
    ssc = new StreamingContext(master, appName, batchDuration)
    val inputStream = addInputStream(ssc)
    inputStream.map(x => x).register()

    // test whether start() blocks indefinitely or not
    failAfter(2000 millis) {
      ssc.start()
    }

    // test whether awaitTermination() exits after give amount of time
    failAfter(1000 millis) {
      ssc.awaitTerminationOrTimeout(500)
    }

    // test whether awaitTermination() does not exit if not time is given
    val exception = intercept[Exception] {
      failAfter(1000 millis) {
        ssc.awaitTermination()
        throw new Exception("Did not wait for stop")
      }
    }
    assert(exception.isInstanceOf[TestFailedDueToTimeoutException], "Did not wait for stop")

    // test whether wait exits if context is stopped
    failAfter(10000 millis) { // 10 seconds because spark takes a long time to shutdown
      new Thread() {
        override def run() {
          Thread.sleep(500)
          ssc.stop()
        }
      }.start()
      ssc.awaitTermination()
    }
  }

  test("awaitTermination after stop") {
    ssc = new StreamingContext(master, appName, batchDuration)
    val inputStream = addInputStream(ssc)
    inputStream.map(x => x).register()

    failAfter(10000 millis) {
      ssc.start()
      ssc.stop()
      ssc.awaitTermination()
    }
  }

  test("awaitTermination with error in task") {
    ssc = new StreamingContext(master, appName, batchDuration)
    val inputStream = addInputStream(ssc)
    inputStream
      .map { x => throw new TestException("error in map task"); x }
      .foreachRDD(_.count())

    val exception = intercept[Exception] {
      ssc.start()
      ssc.awaitTerminationOrTimeout(5000)
    }
    assert(exception.getMessage.contains("map task"), "Expected exception not thrown")
  }

  test("awaitTermination with error in job generation") {
    ssc = new StreamingContext(master, appName, batchDuration)
    val inputStream = addInputStream(ssc)
    inputStream.transform { rdd => throw new TestException("error in transform"); rdd }.register()
    val exception = intercept[TestException] {
      ssc.start()
      ssc.awaitTerminationOrTimeout(5000)
    }
    assert(exception.getMessage.contains("transform"), "Expected exception not thrown")
  }

  test("awaitTerminationOrTimeout") {
    ssc = new StreamingContext(master, appName, batchDuration)
    val inputStream = addInputStream(ssc)
    inputStream.map(x => x).register()

    ssc.start()

    // test whether awaitTerminationOrTimeout() return false after give amount of time
    failAfter(1000 millis) {
      assert(ssc.awaitTerminationOrTimeout(500) === false)
    }

    // test whether awaitTerminationOrTimeout() return true if context is stopped
    failAfter(10000 millis) { // 10 seconds because spark takes a long time to shutdown
      new Thread() {
        override def run() {
          Thread.sleep(500)
          ssc.stop()
        }
      }.start()
      assert(ssc.awaitTerminationOrTimeout(10000) === true)
    }
  }

  test("getOrCreate") {
    val conf = new SparkConf().setMaster(master).setAppName(appName)

    // Function to create StreamingContext that has a config to identify it to be new context
    var newContextCreated = false
    def creatingFunction(): StreamingContext = {
      newContextCreated = true
      new StreamingContext(conf, batchDuration)
    }

    // Call ssc.stop after a body of code
    def testGetOrCreate(body: => Unit): Unit = {
      newContextCreated = false
      try {
        body
      } finally {
        if (ssc != null) {
          ssc.stop()
        }
        ssc = null
      }
    }

    val emptyPath = Utils.createTempDir().getAbsolutePath()

    // getOrCreate should create new context with empty path
    testGetOrCreate {
      ssc = StreamingContext.getOrCreate(emptyPath, creatingFunction _)
      assert(ssc != null, "no context created")
      assert(newContextCreated, "new context not created")
    }

    val corruptedCheckpointPath = createCorruptedCheckpoint()

    // getOrCreate should throw exception with fake checkpoint file and createOnError = false
    intercept[Exception] {
      ssc = StreamingContext.getOrCreate(corruptedCheckpointPath, creatingFunction _)
    }

    // getOrCreate should throw exception with fake checkpoint file
    intercept[Exception] {
      ssc = StreamingContext.getOrCreate(
        corruptedCheckpointPath, creatingFunction _, createOnError = false)
    }

    // getOrCreate should create new context with fake checkpoint file and createOnError = true
    testGetOrCreate {
      ssc = StreamingContext.getOrCreate(
        corruptedCheckpointPath, creatingFunction _, createOnError = true)
      assert(ssc != null, "no context created")
      assert(newContextCreated, "new context not created")
    }

    val checkpointPath = createValidCheckpoint()

    // getOrCreate should recover context with checkpoint path, and recover old configuration
    testGetOrCreate {
      ssc = StreamingContext.getOrCreate(checkpointPath, creatingFunction _)
      assert(ssc != null, "no context created")
      assert(!newContextCreated, "old context not recovered")
      assert(ssc.conf.get("someKey") === "someValue", "checkpointed config not recovered")
    }

    // getOrCreate should recover StreamingContext with existing SparkContext
    testGetOrCreate {
      sc = new SparkContext(conf)
      ssc = StreamingContext.getOrCreate(checkpointPath, creatingFunction _)
      assert(ssc != null, "no context created")
      assert(!newContextCreated, "old context not recovered")
      assert(!ssc.conf.contains("someKey"), "checkpointed config unexpectedly recovered")
    }
  }

  test("getActive and getActiveOrCreate") {
    require(StreamingContext.getActive().isEmpty, "context exists from before")
    sc = new SparkContext(conf)

    var newContextCreated = false

    def creatingFunc(): StreamingContext = {
      newContextCreated = true
      val newSsc = new StreamingContext(sc, batchDuration)
      val input = addInputStream(newSsc)
      input.foreachRDD { rdd => rdd.count }
      newSsc
    }

    def testGetActiveOrCreate(body: => Unit): Unit = {
      newContextCreated = false
      try {
        body
      } finally {

        if (ssc != null) {
          ssc.stop(stopSparkContext = false)
        }
        ssc = null
      }
    }

    // getActiveOrCreate should create new context and getActive should return it only
    // after starting the context
    testGetActiveOrCreate {
      ssc = StreamingContext.getActiveOrCreate(creatingFunc _)
      assert(ssc != null, "no context created")
      assert(newContextCreated === true, "new context not created")
      assert(StreamingContext.getActive().isEmpty,
        "new initialized context returned before starting")
      ssc.start()
      assert(StreamingContext.getActive() === Some(ssc),
        "active context not returned")
      assert(StreamingContext.getActiveOrCreate(creatingFunc _) === ssc,
        "active context not returned")
      ssc.stop()
      assert(StreamingContext.getActive().isEmpty,
        "inactive context returned")
      assert(StreamingContext.getActiveOrCreate(creatingFunc _) !== ssc,
        "inactive context returned")
    }

    // getActiveOrCreate and getActive should return independently created context after activating
    testGetActiveOrCreate {
      ssc = creatingFunc()  // Create
      assert(StreamingContext.getActive().isEmpty,
        "new initialized context returned before starting")
      ssc.start()
      assert(StreamingContext.getActive() === Some(ssc),
        "active context not returned")
      assert(StreamingContext.getActiveOrCreate(creatingFunc _) === ssc,
        "active context not returned")
      ssc.stop()
      assert(StreamingContext.getActive().isEmpty,
        "inactive context returned")
    }
  }

  test("getActiveOrCreate with checkpoint") {
    // Function to create StreamingContext that has a config to identify it to be new context
    var newContextCreated = false
    def creatingFunction(): StreamingContext = {
      newContextCreated = true
      new StreamingContext(conf, batchDuration)
    }

    // Call ssc.stop after a body of code
    def testGetActiveOrCreate(body: => Unit): Unit = {
      require(StreamingContext.getActive().isEmpty) // no active context
      newContextCreated = false
      try {
        body
      } finally {
        if (ssc != null) {
          ssc.stop()
        }
        ssc = null
      }
    }

    val emptyPath = Utils.createTempDir().getAbsolutePath()
    val corruptedCheckpointPath = createCorruptedCheckpoint()
    val checkpointPath = createValidCheckpoint()

    // getActiveOrCreate should return the current active context if there is one
    testGetActiveOrCreate {
      ssc = new StreamingContext(
        conf.clone.set("spark.streaming.clock", "org.apache.spark.util.ManualClock"), batchDuration)
      addInputStream(ssc).register()
      ssc.start()
      val returnedSsc = StreamingContext.getActiveOrCreate(checkpointPath, creatingFunction _)
      assert(!newContextCreated, "new context created instead of returning")
      assert(returnedSsc.eq(ssc), "returned context is not the activated context")
    }

    // getActiveOrCreate should create new context with empty path
    testGetActiveOrCreate {
      ssc = StreamingContext.getActiveOrCreate(emptyPath, creatingFunction _)
      assert(ssc != null, "no context created")
      assert(newContextCreated, "new context not created")
    }

    // getActiveOrCreate should throw exception with fake checkpoint file and createOnError = false
    intercept[Exception] {
      ssc = StreamingContext.getOrCreate(corruptedCheckpointPath, creatingFunction _)
    }

    // getActiveOrCreate should throw exception with fake checkpoint file
    intercept[Exception] {
      ssc = StreamingContext.getActiveOrCreate(
        corruptedCheckpointPath, creatingFunction _, createOnError = false)
    }

    // getActiveOrCreate should create new context with fake
    // checkpoint file and createOnError = true
    testGetActiveOrCreate {
      ssc = StreamingContext.getActiveOrCreate(
        corruptedCheckpointPath, creatingFunction _, createOnError = true)
      assert(ssc != null, "no context created")
      assert(newContextCreated, "new context not created")
    }

    // getActiveOrCreate should recover context with checkpoint path, and recover old configuration
    testGetActiveOrCreate {
      ssc = StreamingContext.getActiveOrCreate(checkpointPath, creatingFunction _)
      assert(ssc != null, "no context created")
      assert(!newContextCreated, "old context not recovered")
      assert(ssc.conf.get("someKey") === "someValue")
    }
  }

  test("multiple streaming contexts") {
    sc = new SparkContext(
      conf.clone.set("spark.streaming.clock", "org.apache.spark.util.ManualClock"))
    ssc = new StreamingContext(sc, Seconds(1))
    val input = addInputStream(ssc)
    input.foreachRDD { rdd => rdd.count }
    ssc.start()

    // Creating another streaming context should not create errors
    val anotherSsc = new StreamingContext(sc, Seconds(10))
    val anotherInput = addInputStream(anotherSsc)
    anotherInput.foreachRDD { rdd => rdd.count }

    val exception = intercept[IllegalStateException] {
      anotherSsc.start()
    }
    assert(exception.getMessage.contains("StreamingContext"), "Did not get the right exception")
  }

  test("DStream and generated RDD creation sites") {
    testPackage.test()
  }

  test("throw exception on using active or stopped context") {
    val conf = new SparkConf()
      .setMaster(master)
      .setAppName(appName)
      .set("spark.streaming.clock", "org.apache.spark.util.ManualClock")
    ssc = new StreamingContext(conf, batchDuration)
    require(ssc.getState() === StreamingContextState.INITIALIZED)
    val input = addInputStream(ssc)
    val transformed = input.map { x => x}
    transformed.foreachRDD { rdd => rdd.count }

    def testForException(clue: String, expectedErrorMsg: String)(body: => Unit): Unit = {
      withClue(clue) {
        val ex = intercept[IllegalStateException] {
          body
        }
        assert(ex.getMessage.toLowerCase().contains(expectedErrorMsg))
      }
    }

    ssc.start()
    require(ssc.getState() === StreamingContextState.ACTIVE)
    testForException("no error on adding input after start", "start") {
      addInputStream(ssc) }
    testForException("no error on adding transformation after start", "start") {
      input.map { x => x * 2 } }
    testForException("no error on adding output operation after start", "start") {
      transformed.foreachRDD { rdd => rdd.collect() } }

    ssc.stop()
    require(ssc.getState() === StreamingContextState.STOPPED)
    testForException("no error on adding input after stop", "stop") {
      addInputStream(ssc) }
    testForException("no error on adding transformation after stop", "stop") {
      input.map { x => x * 2 } }
    testForException("no error on adding output operation after stop", "stop") {
      transformed.foreachRDD { rdd => rdd.collect() } }
  }

  test("queueStream doesn't support checkpointing") {
    val checkpointDir = Utils.createTempDir()
    ssc = new StreamingContext(master, appName, batchDuration)
    val rdd = ssc.sparkContext.parallelize(1 to 10)
    ssc.queueStream[Int](Queue(rdd)).print()
    ssc.checkpoint(checkpointDir.getAbsolutePath)
    val e = intercept[NotSerializableException] {
      ssc.start()
    }
    // StreamingContext.validate changes the message, so use "contains" here
    assert(e.getMessage.contains("queueStream doesn't support checkpointing"))
  }

  def addInputStream(s: StreamingContext): DStream[Int] = {
    val input = (1 to 100).map(i => 1 to i)
    val inputStream = new TestInputStream(s, input, 1)
    inputStream
  }

  def createValidCheckpoint(): String = {
    val testDirectory = Utils.createTempDir().getAbsolutePath()
    val checkpointDirectory = Utils.createTempDir().getAbsolutePath()
    ssc = new StreamingContext(conf.clone.set("someKey", "someValue"), batchDuration)
    ssc.checkpoint(checkpointDirectory)
    ssc.textFileStream(testDirectory).foreachRDD { rdd => rdd.count() }
    ssc.start()
    eventually(timeout(10000 millis)) {
      assert(Checkpoint.getCheckpointFiles(checkpointDirectory).size > 1)
    }
    ssc.stop()
    checkpointDirectory
  }

  def createCorruptedCheckpoint(): String = {
    val checkpointDirectory = Utils.createTempDir().getAbsolutePath()
    val fakeCheckpointFile = Checkpoint.checkpointFile(checkpointDirectory, Time(1000))
    FileUtils.write(new File(fakeCheckpointFile.toString()), "blablabla")
    assert(Checkpoint.getCheckpointFiles(checkpointDirectory).nonEmpty)
    checkpointDirectory
  }
}

class TestException(msg: String) extends Exception(msg)

/** Custom receiver for testing whether all data received by a receiver gets processed or not */
class TestReceiver extends Receiver[Int](StorageLevel.MEMORY_ONLY) with Logging {

  var receivingThreadOption: Option[Thread] = None

  def onStart() {
    val thread = new Thread() {
      override def run() {
        logInfo("Receiving started")
        while (!isStopped) {
          store(TestReceiver.counter.getAndIncrement)
        }
        logInfo("Receiving stopped at count value of " + TestReceiver.counter.get())
      }
    }
    receivingThreadOption = Some(thread)
    thread.start()
  }

  def onStop() {
    // no clean to be done, the receiving thread should stop on it own
  }
}

object TestReceiver {
  val counter = new AtomicInteger(1)
}

/** Custom receiver for testing whether a slow receiver can be shutdown gracefully or not */
class SlowTestReceiver(totalRecords: Int, recordsPerSecond: Int)
  extends Receiver[Int](StorageLevel.MEMORY_ONLY) with Logging {

  var receivingThreadOption: Option[Thread] = None

  def onStart() {
    val thread = new Thread() {
      override def run() {
        logInfo("Receiving started")
        for(i <- 1 to totalRecords) {
          Thread.sleep(1000 / recordsPerSecond)
          store(i)
        }
        SlowTestReceiver.receivedAllRecords = true
        logInfo(s"Received all $totalRecords records")
      }
    }
    receivingThreadOption = Some(thread)
    thread.start()
  }

  def onStop() {
    // Simulate slow receiver by waiting for all records to be produced
    while (!SlowTestReceiver.receivedAllRecords) {
      Thread.sleep(100)
    }
    // no clean to be done, the receiving thread should stop on it own
  }
}

object SlowTestReceiver {
  var receivedAllRecords = false
}

/** Streaming application for testing DStream and RDD creation sites */
package object testPackage extends Assertions {
  def test() {
    val conf = new SparkConf().setMaster("local").setAppName("CreationSite test")
    val ssc = new StreamingContext(conf , Milliseconds(100))
    try {
      val inputStream = ssc.receiverStream(new TestReceiver)

      // Verify creation site of DStream
      val creationSite = inputStream.creationSite
      assert(creationSite.shortForm.contains("receiverStream") &&
        creationSite.shortForm.contains("StreamingContextSuite")
      )
      assert(creationSite.longForm.contains("testPackage"))

      // Verify creation site of generated RDDs
      var rddGenerated = false
      var rddCreationSiteCorrect = false
      var foreachCallSiteCorrect = false

      inputStream.foreachRDD { rdd =>
        rddCreationSiteCorrect = rdd.creationSite == creationSite
        foreachCallSiteCorrect =
          rdd.sparkContext.getCallSite().shortForm.contains("StreamingContextSuite")
        rddGenerated = true
      }
      ssc.start()

      eventually(timeout(10000 millis), interval(10 millis)) {
        assert(rddGenerated && rddCreationSiteCorrect, "RDD creation site was not correct")
        assert(rddGenerated && foreachCallSiteCorrect, "Call site in foreachRDD was not correct")
      }
    } finally {
      ssc.stop()
    }
  }
}
