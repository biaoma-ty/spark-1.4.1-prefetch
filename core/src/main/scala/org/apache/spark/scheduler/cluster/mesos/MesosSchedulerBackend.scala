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

package org.apache.spark.scheduler.cluster.mesos

import java.io.File
import java.util.{ArrayList => JArrayList, Collections, List => JList}

import scala.collection.JavaConversions._
import scala.collection.mutable.{HashMap, HashSet}

import org.apache.mesos.Protos.{ExecutorInfo => MesosExecutorInfo, TaskInfo => MesosTaskInfo, _}
import org.apache.mesos.protobuf.ByteString
import org.apache.mesos.{Scheduler => MScheduler, _}
import org.apache.spark.executor.MesosExecutorBackend
import org.apache.spark.scheduler._
import org.apache.spark.scheduler.cluster.ExecutorInfo
import org.apache.spark.util.Utils
import org.apache.spark.{SparkContext, SparkException, TaskState}

/**
 * A SchedulerBackend for running fine-grained tasks on Mesos. Each Spark task is mapped to a
 * separate Mesos task, allowing multiple applications to share cluster nodes both in space (tasks
 * from multiple apps can run on different cores) and in time (a core can switch ownership).
 */
private[spark] class MesosSchedulerBackend(
    scheduler: TaskSchedulerImpl,
    sc: SparkContext,
    master: String)
  extends SchedulerBackend
  with MScheduler
  with MesosSchedulerUtils {

  // Which slave IDs we have executors on
  val slaveIdsWithExecutors = new HashSet[String]
  val taskIdToSlaveId = new HashMap[Long, String]

  // An ExecutorInfo for our tasks
  var execArgs: Array[Byte] = null

  var classLoader: ClassLoader = null

  // The listener bus to publish executor added/removed events.
  val listenerBus = sc.listenerBus

  private[mesos] val mesosExecutorCores = sc.conf.getDouble("spark.mesos.mesosExecutor.cores", 1)

  @volatile var appId: String = _

  override def start() {
    val fwInfo = FrameworkInfo.newBuilder().setUser(sc.sparkUser).setName(sc.appName).build()
    classLoader = Thread.currentThread.getContextClassLoader
    startScheduler(master, MesosSchedulerBackend.this, fwInfo)
  }

  def createExecutorInfo(execId: String): MesosExecutorInfo = {
    val executorSparkHome = sc.conf.getOption("spark.mesos.executor.home")
      .orElse(sc.getSparkHome()) // Fall back to driver Spark home for backward compatibility
      .getOrElse {
        throw new SparkException("Executor Spark home `spark.mesos.executor.home` is not set!")
      }
    val environment = Environment.newBuilder()
    sc.conf.getOption("spark.executor.extraClassPath").foreach { cp =>
      environment.addVariables(
        Environment.Variable.newBuilder().setName("SPARK_CLASSPATH").setValue(cp).build())
    }
    val extraJavaOpts = sc.conf.getOption("spark.executor.extraJavaOptions").getOrElse("")

    val prefixEnv = sc.conf.getOption("spark.executor.extraLibraryPath").map { p =>
      Utils.libraryPathEnvPrefix(Seq(p))
    }.getOrElse("")

    environment.addVariables(
      Environment.Variable.newBuilder()
        .setName("SPARK_EXECUTOR_OPTS")
        .setValue(extraJavaOpts)
        .build())
    sc.executorEnvs.foreach { case (key, value) =>
      environment.addVariables(Environment.Variable.newBuilder()
        .setName(key)
        .setValue(value)
        .build())
    }
    val command = CommandInfo.newBuilder()
      .setEnvironment(environment)
    val uri = sc.conf.getOption("spark.executor.uri")
      .orElse(Option(System.getenv("SPARK_EXECUTOR_URI")))

    val executorBackendName = classOf[MesosExecutorBackend].getName
    if (uri.isEmpty) {
      val executorPath = new File(executorSparkHome, "/bin/spark-class").getCanonicalPath
      command.setValue(s"$prefixEnv $executorPath $executorBackendName")
    } else {
      // Grab everything to the first '.'. We'll use that and '*' to
      // glob the directory "correctly".
      val basename = uri.get.split('/').last.split('.').head
      command.setValue(s"cd ${basename}*; $prefixEnv ./bin/spark-class $executorBackendName")
      command.addUris(CommandInfo.URI.newBuilder().setValue(uri.get))
    }
    val cpus = Resource.newBuilder()
      .setName("cpus")
      .setType(Value.Type.SCALAR)
      .setScalar(Value.Scalar.newBuilder()
        .setValue(mesosExecutorCores).build())
      .build()
    val memory = Resource.newBuilder()
      .setName("mem")
      .setType(Value.Type.SCALAR)
      .setScalar(
        Value.Scalar.newBuilder()
          .setValue(MemoryUtils.calculateTotalMemory(sc)).build())
      .build()
    val executorInfo = MesosExecutorInfo.newBuilder()
      .setExecutorId(ExecutorID.newBuilder().setValue(execId).build())
      .setCommand(command)
      .setData(ByteString.copyFrom(createExecArg()))
      .addResources(cpus)
      .addResources(memory)

    sc.conf.getOption("spark.mesos.executor.docker.image").foreach { image =>
      MesosSchedulerBackendUtil
        .setupContainerBuilderDockerInfo(image, sc.conf, executorInfo.getContainerBuilder())
    }

    executorInfo.build()
  }

  /**
   * Create and serialize the executor argument to pass to Mesos. Our executor arg is an array
   * containing all the spark.* system properties in the form of (String, String) pairs.
   */
  private def createExecArg(): Array[Byte] = {
    if (execArgs == null) {
      val props = new HashMap[String, String]
      for ((key, value) <- sc.conf.getAll) {
        props(key) = value
      }
      // Serialize the map as an array of (String, String) pairs
      execArgs = Utils.serialize(props.toArray)
    }
    execArgs
  }

  override def offerRescinded(d: SchedulerDriver, o: OfferID) {}

  override def registered(d: SchedulerDriver, frameworkId: FrameworkID, masterInfo: MasterInfo) {
    inClassLoader() {
      appId = frameworkId.getValue
      logInfo("Registered as framework ID " + appId)
      markRegistered()
    }
  }

  private def inClassLoader()(fun: => Unit) = {
    val oldClassLoader = Thread.currentThread.getContextClassLoader
    Thread.currentThread.setContextClassLoader(classLoader)
    try {
      fun
    } finally {
      Thread.currentThread.setContextClassLoader(oldClassLoader)
    }
  }

  override def disconnected(d: SchedulerDriver) {}

  override def reregistered(d: SchedulerDriver, masterInfo: MasterInfo) {}

  /**
   * Method called by Mesos to offer resources on slaves. We respond by asking our active task sets
   * for tasks in order of priority. We fill each node with tasks in a round-robin manner so that
   * tasks are balanced across the cluster.
   */
  override def resourceOffers(d: SchedulerDriver, offers: JList[Offer]) {
    inClassLoader() {
      // Fail-fast on offers we know will be rejected
      val (usableOffers, unUsableOffers) = offers.partition { o =>
        val mem = getResource(o.getResourcesList, "mem")
        val cpus = getResource(o.getResourcesList, "cpus")
        val slaveId = o.getSlaveId.getValue
        (mem >= MemoryUtils.calculateTotalMemory(sc) &&
          // need at least 1 for executor, 1 for task
          cpus >= (mesosExecutorCores + scheduler.CPUS_PER_TASK)) ||
          (slaveIdsWithExecutors.contains(slaveId) &&
            cpus >= scheduler.CPUS_PER_TASK)
      }

      val workerOffers = usableOffers.map { o =>
        val cpus = if (slaveIdsWithExecutors.contains(o.getSlaveId.getValue)) {
          getResource(o.getResourcesList, "cpus").toInt
        } else {
          // If the Mesos executor has not been started on this slave yet, set aside a few
          // cores for the Mesos executor by offering fewer cores to the Spark executor
          (getResource(o.getResourcesList, "cpus") - mesosExecutorCores).toInt
        }
        new WorkerOffer(
          o.getSlaveId.getValue,
          o.getHostname,
          cpus)
      }

      val slaveIdToOffer = usableOffers.map(o => o.getSlaveId.getValue -> o).toMap
      val slaveIdToWorkerOffer = workerOffers.map(o => o.executorId -> o).toMap

      val mesosTasks = new HashMap[String, JArrayList[MesosTaskInfo]]

      val slavesIdsOfAcceptedOffers = HashSet[String]()

      // Call into the TaskSchedulerImpl
      val acceptedOffers = scheduler.resourceOffers(workerOffers).filter(!_.isEmpty)
      acceptedOffers
        .foreach { offer =>
          offer.foreach { taskDesc =>
            val slaveId = taskDesc.executorId
            slaveIdsWithExecutors += slaveId
            slavesIdsOfAcceptedOffers += slaveId
            taskIdToSlaveId(taskDesc.taskId) = slaveId
            mesosTasks.getOrElseUpdate(slaveId, new JArrayList[MesosTaskInfo])
              .add(createMesosTask(taskDesc, slaveId))
          }
        }

      // Reply to the offers
      val filters = Filters.newBuilder().setRefuseSeconds(1).build() // TODO: lower timeout?

      mesosTasks.foreach { case (slaveId, tasks) =>
        slaveIdToWorkerOffer.get(slaveId).foreach(o =>
          listenerBus.post(SparkListenerExecutorAdded(System.currentTimeMillis(), slaveId,
            // TODO: Add support for log urls for Mesos
            new ExecutorInfo(o.host, o.cores, Map.empty)))
        )
        d.launchTasks(Collections.singleton(slaveIdToOffer(slaveId).getId), tasks, filters)
      }

      // Decline offers that weren't used
      // NOTE: This logic assumes that we only get a single offer for each host in a given batch
      for (o <- usableOffers if !slavesIdsOfAcceptedOffers.contains(o.getSlaveId.getValue)) {
        d.declineOffer(o.getId)
      }

      // Decline offers we ruled out immediately
      unUsableOffers.foreach(o => d.declineOffer(o.getId))
    }
  }

  /** Turn a Spark TaskDescription into a Mesos task */
  def createMesosTask(task: TaskDescription, slaveId: String): MesosTaskInfo = {
    val taskId = TaskID.newBuilder().setValue(task.taskId.toString).build()
    val cpuResource = Resource.newBuilder()
      .setName("cpus")
      .setType(Value.Type.SCALAR)
      .setScalar(Value.Scalar.newBuilder().setValue(scheduler.CPUS_PER_TASK).build())
      .build()
    MesosTaskInfo.newBuilder()
      .setTaskId(taskId)
      .setSlaveId(SlaveID.newBuilder().setValue(slaveId).build())
      .setExecutor(createExecutorInfo(slaveId))
      .setName(task.name)
      .addResources(cpuResource)
      .setData(MesosTaskLaunchData(task.serializedTask, task.attemptNumber).toByteString)
      .build()
  }

  override def statusUpdate(d: SchedulerDriver, status: TaskStatus) {
    inClassLoader() {
      val tid = status.getTaskId.getValue.toLong
      val state = TaskState.fromMesos(status.getState)
      synchronized {
        if (TaskState.isFailed(TaskState.fromMesos(status.getState))
          && taskIdToSlaveId.contains(tid)) {
          // We lost the executor on this slave, so remember that it's gone
          removeExecutor(taskIdToSlaveId(tid), "Lost executor")
        }
        if (TaskState.isFinished(state)) {
          taskIdToSlaveId.remove(tid)
        }
      }
      scheduler.statusUpdate(tid, state, status.getData.asReadOnlyByteBuffer)
    }
  }

  override def error(d: SchedulerDriver, message: String) {
    inClassLoader() {
      logError("Mesos error: " + message)
      scheduler.error(message)
    }
  }

  override def stop() {
    if (mesosDriver != null) {
      mesosDriver.stop()
    }
  }

  override def reviveOffers() {
    mesosDriver.reviveOffers()
  }

  override def frameworkMessage(d: SchedulerDriver, e: ExecutorID, s: SlaveID, b: Array[Byte]) {}

  /**
   * Remove executor associated with slaveId in a thread safe manner.
   */
  private def removeExecutor(slaveId: String, reason: String) = {
    synchronized {
      listenerBus.post(SparkListenerExecutorRemoved(System.currentTimeMillis(), slaveId, reason))
      slaveIdsWithExecutors -= slaveId
    }
  }

  private def recordSlaveLost(d: SchedulerDriver, slaveId: SlaveID, reason: ExecutorLossReason) {
    inClassLoader() {
      logInfo("Mesos slave lost: " + slaveId.getValue)
      removeExecutor(slaveId.getValue, reason.toString)
      scheduler.executorLost(slaveId.getValue, reason)
    }
  }

  override def slaveLost(d: SchedulerDriver, slaveId: SlaveID) {
    recordSlaveLost(d, slaveId, SlaveLost())
  }

  override def executorLost(d: SchedulerDriver, executorId: ExecutorID,
                            slaveId: SlaveID, status: Int) {
    logInfo("Executor lost: %s, marking slave %s as lost".format(executorId.getValue,
                                                                 slaveId.getValue))
    recordSlaveLost(d, slaveId, ExecutorExited(status))
  }

  override def killTask(taskId: Long, executorId: String, interruptThread: Boolean): Unit = {
    mesosDriver.killTask(
      TaskID.newBuilder()
        .setValue(taskId.toString).build()
    )
  }

  // TODO: query Mesos for number of cores
  override def defaultParallelism(): Int = sc.conf.getInt("spark.default.parallelism", 8)

  override def applicationId(): String =
    Option(appId).getOrElse {
      logWarning("Application ID is not initialized yet.")
      super.applicationId
    }

}
