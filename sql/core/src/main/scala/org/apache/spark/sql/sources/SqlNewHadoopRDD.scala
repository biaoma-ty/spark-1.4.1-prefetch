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

package org.apache.spark.sql.sources

import java.text.SimpleDateFormat
import java.util.Date

import org.apache.hadoop.conf.{Configurable, Configuration}
import org.apache.hadoop.io.Writable
import org.apache.hadoop.mapreduce._
import org.apache.hadoop.mapreduce.lib.input.{CombineFileSplit, FileSplit}
import org.apache.spark.broadcast.Broadcast

import org.apache.spark.{Partition => SparkPartition, _}
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.executor.DataReadMethod
import org.apache.spark.mapreduce.SparkHadoopMapReduceUtil
import org.apache.spark.rdd.{RDD, HadoopRDD}
import org.apache.spark.rdd.NewHadoopRDD.NewHadoopMapPartitionsWithSplitRDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.util.Utils

import scala.reflect.ClassTag

private[spark] class SqlNewHadoopPartition(
    rddId: Int,
    val index: Int,
    @transient rawSplit: InputSplit with Writable)
  extends SparkPartition {

  val serializableHadoopSplit = new SerializableWritable(rawSplit)

  override def hashCode(): Int = 41 * (41 + rddId) + index
}

/**
 * An RDD that provides core functionality for reading data stored in Hadoop (e.g., files in HDFS,
 * sources in HBase, or S3), using the new MapReduce API (`org.apache.hadoop.mapreduce`).
 * It is based on [[org.apache.spark.rdd.NewHadoopRDD]]. It has three additions.
 * 1. A shared broadcast Hadoop Configuration.
 * 2. An optional closure `initDriverSideJobFuncOpt` that set configurations at the driver side
 *    to the shared Hadoop Configuration.
 * 3. An optional closure `initLocalJobFuncOpt` that set configurations at both the driver side
 *    and the executor side to the shared Hadoop Configuration.
 *
 * Note: This is RDD is basically a cloned version of [[org.apache.spark.rdd.NewHadoopRDD]] with
 * changes based on [[org.apache.spark.rdd.HadoopRDD]]. In future, this functionality will be
 * folded into core.
 */
private[sql] class SqlNewHadoopRDD[K, V](
    @transient sc : SparkContext,
    broadcastedConf: Broadcast[SerializableWritable[Configuration]],
    @transient initDriverSideJobFuncOpt: Option[Job => Unit],
    initLocalJobFuncOpt: Option[Job => Unit],
    inputFormatClass: Class[_ <: InputFormat[K, V]],
    keyClass: Class[K],
    valueClass: Class[V])
  extends RDD[(K, V)](sc, Nil)
  with SparkHadoopMapReduceUtil
  with Logging {

  protected def getJob(): Job = {
    val conf: Configuration = broadcastedConf.value.value
    // "new Job" will make a copy of the conf. Then, it is
    // safe to mutate conf properties with initLocalJobFuncOpt
    // and initDriverSideJobFuncOpt.
    val newJob = new Job(conf)
    initLocalJobFuncOpt.map(f => f(newJob))
    newJob
  }

  def getConf(isDriverSide: Boolean): Configuration = {
    val job = getJob()
    if (isDriverSide) {
      initDriverSideJobFuncOpt.map(f => f(job))
    }
    job.getConfiguration
  }

  private val jobTrackerId: String = {
    val formatter = new SimpleDateFormat("yyyyMMddHHmm")
    formatter.format(new Date())
  }

  @transient protected val jobId = new JobID(jobTrackerId, id)

  override def getPartitions: Array[SparkPartition] = {
    val conf = getConf(isDriverSide = true)
    val inputFormat = inputFormatClass.newInstance
    inputFormat match {
      case configurable: Configurable =>
        configurable.setConf(conf)
      case _ =>
    }
    val jobContext = newJobContext(conf, jobId)
    val rawSplits = inputFormat.getSplits(jobContext).toArray
    val result = new Array[SparkPartition](rawSplits.size)
    for (i <- 0 until rawSplits.size) {
      result(i) =
        new SqlNewHadoopPartition(id, i, rawSplits(i).asInstanceOf[InputSplit with Writable])
    }
    result
  }

  override def compute(
      theSplit: SparkPartition,
      context: TaskContext): InterruptibleIterator[(K, V)] = {
    val iter = new Iterator[(K, V)] {
      val split = theSplit.asInstanceOf[SqlNewHadoopPartition]
      logInfo("Input split: " + split.serializableHadoopSplit)
      val conf = getConf(isDriverSide = false)

      val inputMetrics = context.taskMetrics
        .getInputMetricsForReadMethod(DataReadMethod.Hadoop)

      // Find a function that will return the FileSystem bytes read by this thread. Do this before
      // creating RecordReader, because RecordReader's constructor might read some bytes
      val bytesReadCallback = inputMetrics.bytesReadCallback.orElse {
        split.serializableHadoopSplit.value match {
          case _: FileSplit | _: CombineFileSplit =>
            SparkHadoopUtil.get.getFSBytesReadOnThreadCallback()
          case _ => None
        }
      }
      inputMetrics.setBytesReadCallback(bytesReadCallback)

      val attemptId = newTaskAttemptID(jobTrackerId, id, isMap = true, split.index, 0)
      val hadoopAttemptContext = newTaskAttemptContext(conf, attemptId)
      val format = inputFormatClass.newInstance
      format match {
        case configurable: Configurable =>
          configurable.setConf(conf)
        case _ =>
      }
      val reader = format.createRecordReader(
        split.serializableHadoopSplit.value, hadoopAttemptContext)
      reader.initialize(split.serializableHadoopSplit.value, hadoopAttemptContext)

      // Register an on-task-completion callback to close the input stream.
      context.addTaskCompletionListener(context => close())
      var havePair = false
      var finished = false
      var recordsSinceMetricsUpdate = 0

      override def hasNext: Boolean = {
        if (!finished && !havePair) {
          finished = !reader.nextKeyValue
          havePair = !finished
        }
        !finished
      }

      override def next(): (K, V) = {
        if (!hasNext) {
          throw new java.util.NoSuchElementException("End of stream")
        }
        havePair = false
        if (!finished) {
          inputMetrics.incRecordsRead(1)
        }
        (reader.getCurrentKey, reader.getCurrentValue)
      }

      private def close() {
        try {
          reader.close()
          if (bytesReadCallback.isDefined) {
            inputMetrics.updateBytesRead()
          } else if (split.serializableHadoopSplit.value.isInstanceOf[FileSplit] ||
                     split.serializableHadoopSplit.value.isInstanceOf[CombineFileSplit]) {
            // If we can't get the bytes read from the FS stats, fall back to the split size,
            // which may be inaccurate.
            try {
              inputMetrics.incBytesRead(split.serializableHadoopSplit.value.getLength)
            } catch {
              case e: java.io.IOException =>
                logWarning("Unable to get input size to set InputMetrics for task", e)
            }
          }
        } catch {
          case e: Exception => {
            if (!Utils.inShutdown()) {
              logWarning("Exception in RecordReader.close()", e)
            }
          }
        }
      }
    }
    new InterruptibleIterator(context, iter)
  }

  /** Maps over a partition, providing the InputSplit that was used as the base of the partition. */
  @DeveloperApi
  def mapPartitionsWithInputSplit[U: ClassTag](
      f: (InputSplit, Iterator[(K, V)]) => Iterator[U],
      preservesPartitioning: Boolean = false): RDD[U] = {
    new NewHadoopMapPartitionsWithSplitRDD(this, f, preservesPartitioning)
  }

  override def getPreferredLocations(hsplit: SparkPartition): Seq[String] = {
    val split = hsplit.asInstanceOf[SqlNewHadoopPartition].serializableHadoopSplit.value
    val locs = HadoopRDD.SPLIT_INFO_REFLECTIONS match {
      case Some(c) =>
        try {
          val infos = c.newGetLocationInfo.invoke(split).asInstanceOf[Array[AnyRef]]
          Some(HadoopRDD.convertSplitLocationInfo(infos))
        } catch {
          case e : Exception =>
            logDebug("Failed to use InputSplit#getLocationInfo.", e)
            None
        }
      case None => None
    }
    locs.getOrElse(split.getLocations.filter(_ != "localhost"))
  }

  override def persist(storageLevel: StorageLevel): this.type = {
    if (storageLevel.deserialized) {
      logWarning("Caching NewHadoopRDDs as deserialized objects usually leads to undesired" +
        " behavior because Hadoop's RecordReader reuses the same Writable object for all records." +
        " Use a map transformation to make copies of the records.")
    }
    super.persist(storageLevel)
  }
}

private[spark] object SqlNewHadoopRDD {
  /**
   * Analogous to [[org.apache.spark.rdd.MapPartitionsRDD]], but passes in an InputSplit to
   * the given function rather than the index of the partition.
   */
  private[spark] class NewHadoopMapPartitionsWithSplitRDD[U: ClassTag, T: ClassTag](
      prev: RDD[T],
      f: (InputSplit, Iterator[T]) => Iterator[U],
      preservesPartitioning: Boolean = false)
    extends RDD[U](prev) {

    override val partitioner = if (preservesPartitioning) firstParent[T].partitioner else None

    override def getPartitions: Array[SparkPartition] = firstParent[T].partitions

    override def compute(split: SparkPartition, context: TaskContext): Iterator[U] = {
      val partition = split.asInstanceOf[SqlNewHadoopPartition]
      val inputSplit = partition.serializableHadoopSplit.value
      f(inputSplit, firstParent[T].iterator(split, context))
    }
  }
}
