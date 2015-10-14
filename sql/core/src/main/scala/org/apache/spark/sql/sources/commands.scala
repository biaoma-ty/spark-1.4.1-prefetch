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

import java.util.{UUID, Date}

import scala.collection.mutable

import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapreduce._
import org.apache.hadoop.mapreduce.lib.output.{FileOutputCommitter => MapReduceFileOutputCommitter, FileOutputFormat}
import parquet.hadoop.util.ContextUtil

import org.apache.spark._
import org.apache.spark.mapred.SparkHadoopMapRedUtil
import org.apache.spark.mapreduce.SparkHadoopMapReduceUtil
import org.apache.spark.sql.catalyst.CatalystTypeConverters
import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.codegen.GenerateProjection
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, Project}
import org.apache.spark.sql.execution.RunnableCommand
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SQLConf, SQLContext, SaveMode}

private[sql] case class InsertIntoDataSource(
    logicalRelation: LogicalRelation,
    query: LogicalPlan,
    overwrite: Boolean)
  extends RunnableCommand {

  override def run(sqlContext: SQLContext): Seq[Row] = {
    val relation = logicalRelation.relation.asInstanceOf[InsertableRelation]
    val data = DataFrame(sqlContext, query)
    // Apply the schema of the existing table to the new data.
    val df = sqlContext.createDataFrame(
      data.queryExecution.toRdd, logicalRelation.schema, needsConversion = false)
    relation.insert(df, overwrite)

    // Invalidate the cache.
    sqlContext.cacheManager.invalidateCache(logicalRelation)

    Seq.empty[Row]
  }
}

/**
 * A command for writing data to a [[HadoopFsRelation]].  Supports both overwriting and appending.
 * Writing to dynamic partitions is also supported.  Each [[InsertIntoHadoopFsRelation]] issues a
 * single write job, and owns a UUID that identifies this job.  Each concrete implementation of
 * [[HadoopFsRelation]] should use this UUID together with task id to generate unique file path for
 * each task output file.  This UUID is passed to executor side via a property named
 * `spark.sql.sources.writeJobUUID`.
 *
 * Different writer containers, [[DefaultWriterContainer]] and [[DynamicPartitionWriterContainer]]
 * are used to write to normal tables and tables with dynamic partitions.
 *
 * Basic work flow of this command is:
 *
 *   1. Driver side setup, including output committer initialization and data source specific
 *      preparation work for the write job to be issued.
 *   2. Issues a write job consists of one or more executor side tasks, each of which writes all
 *      rows within an RDD partition.
 *   3. If no exception is thrown in a task, commits that task, otherwise aborts that task;  If any
 *      exception is thrown during task commitment, also aborts that task.
 *   4. If all tasks are committed, commit the job, otherwise aborts the job;  If any exception is
 *      thrown during job commitment, also aborts the job.
 */
private[sql] case class InsertIntoHadoopFsRelation(
    @transient relation: HadoopFsRelation,
    @transient query: LogicalPlan,
    mode: SaveMode)
  extends RunnableCommand {

  override def run(sqlContext: SQLContext): Seq[Row] = {
    require(
      relation.paths.length == 1,
      s"Cannot write to multiple destinations: ${relation.paths.mkString(",")}")

    val hadoopConf = sqlContext.sparkContext.hadoopConfiguration
    val outputPath = new Path(relation.paths.head)
    val fs = outputPath.getFileSystem(hadoopConf)
    val qualifiedOutputPath = outputPath.makeQualified(fs.getUri, fs.getWorkingDirectory)

    val pathExists = fs.exists(qualifiedOutputPath)
    val doInsertion = (mode, pathExists) match {
      case (SaveMode.ErrorIfExists, true) =>
        sys.error(s"path $qualifiedOutputPath already exists.")
      case (SaveMode.Overwrite, true) =>
        fs.delete(qualifiedOutputPath, true)
        true
      case (SaveMode.Append, _) | (SaveMode.Overwrite, _) | (SaveMode.ErrorIfExists, false) =>
        true
      case (SaveMode.Ignore, exists) =>
        !exists
    }
    // If we are appending data to an existing dir.
    val isAppend = (pathExists) && (mode == SaveMode.Append)

    if (doInsertion) {
      val job = new Job(hadoopConf)
      job.setOutputKeyClass(classOf[Void])
      job.setOutputValueClass(classOf[Row])
      FileOutputFormat.setOutputPath(job, qualifiedOutputPath)

      // We create a DataFrame by applying the schema of relation to the data to make sure.
      // We are writing data based on the expected schema,
      val df = {
        // For partitioned relation r, r.schema's column ordering can be different from the column
        // ordering of data.logicalPlan (partition columns are all moved after data column). We
        // need a Project to adjust the ordering, so that inside InsertIntoHadoopFsRelation, we can
        // safely apply the schema of r.schema to the data.
        val project = Project(
          relation.schema.map(field => new UnresolvedAttribute(Seq(field.name))), query)

        sqlContext.createDataFrame(
          DataFrame(sqlContext, project).queryExecution.toRdd,
          relation.schema,
          needsConversion = false)
      }

      val partitionColumns = relation.partitionColumns.fieldNames
      if (partitionColumns.isEmpty) {
        insert(new DefaultWriterContainer(relation, job, isAppend), df)
      } else {
        val writerContainer = new DynamicPartitionWriterContainer(
          relation, job, partitionColumns, PartitioningUtils.DEFAULT_PARTITION_NAME, isAppend)
        insertWithDynamicPartitions(sqlContext, writerContainer, df, partitionColumns)
      }
    }

    Seq.empty[Row]
  }

  private def insert(writerContainer: BaseWriterContainer, df: DataFrame): Unit = {
    // Uses local vals for serialization
    val needsConversion = relation.needConversion
    val dataSchema = relation.dataSchema

    // This call shouldn't be put into the `try` block below because it only initializes and
    // prepares the job, any exception thrown from here shouldn't cause abortJob() to be called.
    writerContainer.driverSideSetup()

    try {
      df.sqlContext.sparkContext.runJob(df.queryExecution.executedPlan.execute(), writeRows _)
      writerContainer.commitJob()
      relation.refresh()
    } catch { case cause: Throwable =>
      logError("Aborting job.", cause)
      writerContainer.abortJob()
      throw new SparkException("Job aborted.", cause)
    }

    def writeRows(taskContext: TaskContext, iterator: Iterator[Row]): Unit = {
      // If anything below fails, we should abort the task.
      try {
        writerContainer.executorSideSetup(taskContext)

        if (needsConversion) {
          val converter = CatalystTypeConverters.createToScalaConverter(dataSchema)
          while (iterator.hasNext) {
            val row = converter(iterator.next()).asInstanceOf[Row]
            writerContainer.outputWriterForRow(row).write(row)
          }
        } else {
          while (iterator.hasNext) {
            val row = iterator.next()
            writerContainer.outputWriterForRow(row).write(row)
          }
        }

        writerContainer.commitTask()
      } catch { case cause: Throwable =>
        logError("Aborting task.", cause)
        writerContainer.abortTask()
        throw new SparkException("Task failed while writing rows.", cause)
      }
    }
  }

  private def insertWithDynamicPartitions(
      sqlContext: SQLContext,
      writerContainer: BaseWriterContainer,
      df: DataFrame,
      partitionColumns: Array[String]): Unit = {
    // Uses a local val for serialization
    val needsConversion = relation.needConversion
    val dataSchema = relation.dataSchema

    require(
      df.schema == relation.schema,
      s"""DataFrame must have the same schema as the relation to which is inserted.
         |DataFrame schema: ${df.schema}
         |Relation schema: ${relation.schema}
       """.stripMargin)

    val partitionColumnsInSpec = relation.partitionColumns.fieldNames
    require(
      partitionColumnsInSpec.sameElements(partitionColumns),
      s"""Partition columns mismatch.
         |Expected: ${partitionColumnsInSpec.mkString(", ")}
         |Actual: ${partitionColumns.mkString(", ")}
       """.stripMargin)

    val output = df.queryExecution.executedPlan.output
    val (partitionOutput, dataOutput) = output.partition(a => partitionColumns.contains(a.name))
    val codegenEnabled = df.sqlContext.conf.codegenEnabled

    // This call shouldn't be put into the `try` block below because it only initializes and
    // prepares the job, any exception thrown from here shouldn't cause abortJob() to be called.
    writerContainer.driverSideSetup()

    try {
      df.sqlContext.sparkContext.runJob(df.queryExecution.executedPlan.execute(), writeRows _)
      writerContainer.commitJob()
      relation.refresh()
    } catch { case cause: Throwable =>
      logError("Aborting job.", cause)
      writerContainer.abortJob()
      throw new SparkException("Job aborted.", cause)
    }

    def writeRows(taskContext: TaskContext, iterator: Iterator[Row]): Unit = {
      // If anything below fails, we should abort the task.
      try {
        writerContainer.executorSideSetup(taskContext)

        val partitionProj = newProjection(codegenEnabled, partitionOutput, output)
        val dataProj = newProjection(codegenEnabled, dataOutput, output)

        if (needsConversion) {
          val converter = CatalystTypeConverters.createToScalaConverter(dataSchema)
          while (iterator.hasNext) {
            val row = iterator.next()
            val partitionPart = partitionProj(row)
            val dataPart = dataProj(row)
            val convertedDataPart = converter(dataPart).asInstanceOf[Row]
            writerContainer.outputWriterForRow(partitionPart).write(convertedDataPart)
          }
        } else {
          val partitionSchema = StructType.fromAttributes(partitionOutput)
          val converter = CatalystTypeConverters.createToScalaConverter(partitionSchema)
          while (iterator.hasNext) {
            val row = iterator.next()
            val partitionPart = converter(partitionProj(row)).asInstanceOf[Row]
            val dataPart = dataProj(row)
            writerContainer.outputWriterForRow(partitionPart).write(dataPart)
          }
        }

        writerContainer.commitTask()
      } catch { case cause: Throwable =>
        logError("Aborting task.", cause)
        writerContainer.abortTask()
        throw new SparkException("Task failed while writing rows.", cause)
      }
    }
  }

  // This is copied from SparkPlan, probably should move this to a more general place.
  private def newProjection(
      codegenEnabled: Boolean,
      expressions: Seq[Expression],
      inputSchema: Seq[Attribute]): Projection = {
    log.debug(
      s"Creating Projection: $expressions, inputSchema: $inputSchema, codegen:$codegenEnabled")
    if (codegenEnabled) {
      GenerateProjection.generate(expressions, inputSchema)
    } else {
      new InterpretedProjection(expressions, inputSchema)
    }
  }
}

private[sql] abstract class BaseWriterContainer(
    @transient val relation: HadoopFsRelation,
    @transient job: Job,
    isAppend: Boolean)
  extends SparkHadoopMapReduceUtil
  with Logging
  with Serializable {

  protected val serializableConf = new SerializableWritable(ContextUtil.getConfiguration(job))

  // This UUID is used to avoid output file name collision between different appending write jobs.
  // These jobs may belong to different SparkContext instances. Concrete data source implementations
  // may use this UUID to generate unique file names (e.g., `part-r-<task-id>-<job-uuid>.parquet`).
  //  The reason why this ID is used to identify a job rather than a single task output file is
  // that, speculative tasks must generate the same output file name as the original task.
  private val uniqueWriteJobId = UUID.randomUUID()

  // This is only used on driver side.
  @transient private val jobContext: JobContext = job

  // The following fields are initialized and used on both driver and executor side.
  @transient protected var outputCommitter: OutputCommitter = _
  @transient private var jobId: JobID = _
  @transient private var taskId: TaskID = _
  @transient private var taskAttemptId: TaskAttemptID = _
  @transient protected var taskAttemptContext: TaskAttemptContext = _

  protected val outputPath: String = {
    assert(
      relation.paths.length == 1,
      s"Cannot write to multiple destinations: ${relation.paths.mkString(",")}")
    relation.paths.head
  }

  protected val dataSchema = relation.dataSchema

  protected var outputWriterFactory: OutputWriterFactory = _

  private var outputFormatClass: Class[_ <: OutputFormat[_, _]] = _

  def driverSideSetup(): Unit = {
    setupIDs(0, 0, 0)
    setupConf()

    // This UUID is sent to executor side together with the serialized `Configuration` object within
    // the `Job` instance.  `OutputWriters` on the executor side should use this UUID to generate
    // unique task output files.
    job.getConfiguration.set("spark.sql.sources.writeJobUUID", uniqueWriteJobId.toString)

    // Order of the following two lines is important.  For Hadoop 1, TaskAttemptContext constructor
    // clones the Configuration object passed in.  If we initialize the TaskAttemptContext first,
    // configurations made in prepareJobForWrite(job) are not populated into the TaskAttemptContext.
    //
    // Also, the `prepareJobForWrite` call must happen before initializing output format and output
    // committer, since their initialization involve the job configuration, which can be potentially
    // decorated in `prepareJobForWrite`.
    outputWriterFactory = relation.prepareJobForWrite(job)
    taskAttemptContext = newTaskAttemptContext(serializableConf.value, taskAttemptId)

    outputFormatClass = job.getOutputFormatClass
    outputCommitter = newOutputCommitter(taskAttemptContext)
    outputCommitter.setupJob(jobContext)
  }

  def executorSideSetup(taskContext: TaskContext): Unit = {
    setupIDs(taskContext.stageId(), taskContext.partitionId(), taskContext.attemptNumber())
    setupConf()
    taskAttemptContext = newTaskAttemptContext(serializableConf.value, taskAttemptId)
    outputCommitter = newOutputCommitter(taskAttemptContext)
    outputCommitter.setupTask(taskAttemptContext)
    initWriters()
  }

  protected def getWorkPath: String = {
    outputCommitter match {
      // FileOutputCommitter writes to a temporary location returned by `getWorkPath`.
      case f: MapReduceFileOutputCommitter => f.getWorkPath.toString
      case _ => outputPath
    }
  }

  private def newOutputCommitter(context: TaskAttemptContext): OutputCommitter = {
    val defaultOutputCommitter = outputFormatClass.newInstance().getOutputCommitter(context)

    if (isAppend) {
      // If we are appending data to an existing dir, we will only use the output committer
      // associated with the file output format since it is not safe to use a custom
      // committer for appending. For example, in S3, direct parquet output committer may
      // leave partial data in the destination dir when the the appending job fails.
      logInfo(
        s"Using output committer class ${defaultOutputCommitter.getClass.getCanonicalName} " +
          "for appending.")
      defaultOutputCommitter
    } else {
      val committerClass = context.getConfiguration.getClass(
        SQLConf.OUTPUT_COMMITTER_CLASS, null, classOf[OutputCommitter])

      Option(committerClass).map { clazz =>
        logInfo(s"Using user defined output committer class ${clazz.getCanonicalName}")

        // Every output format based on org.apache.hadoop.mapreduce.lib.output.OutputFormat
        // has an associated output committer. To override this output committer,
        // we will first try to use the output committer set in SQLConf.OUTPUT_COMMITTER_CLASS.
        // If a data source needs to override the output committer, it needs to set the
        // output committer in prepareForWrite method.
        if (classOf[MapReduceFileOutputCommitter].isAssignableFrom(clazz)) {
          // The specified output committer is a FileOutputCommitter.
          // So, we will use the FileOutputCommitter-specified constructor.
          val ctor = clazz.getDeclaredConstructor(classOf[Path], classOf[TaskAttemptContext])
          ctor.newInstance(new Path(outputPath), context)
        } else {
          // The specified output committer is just a OutputCommitter.
          // So, we will use the no-argument constructor.
          val ctor = clazz.getDeclaredConstructor()
          ctor.newInstance()
        }
      }.getOrElse {
        // If output committer class is not set, we will use the one associated with the
        // file output format.
        logInfo(
          s"Using output committer class ${defaultOutputCommitter.getClass.getCanonicalName}")
        defaultOutputCommitter
      }
    }
  }

  private def setupIDs(jobId: Int, splitId: Int, attemptId: Int): Unit = {
    this.jobId = SparkHadoopWriter.createJobID(new Date, jobId)
    this.taskId = new TaskID(this.jobId, true, splitId)
    this.taskAttemptId = new TaskAttemptID(taskId, attemptId)
  }

  private def setupConf(): Unit = {
    serializableConf.value.set("mapred.job.id", jobId.toString)
    serializableConf.value.set("mapred.tip.id", taskAttemptId.getTaskID.toString)
    serializableConf.value.set("mapred.task.id", taskAttemptId.toString)
    serializableConf.value.setBoolean("mapred.task.is.map", true)
    serializableConf.value.setInt("mapred.task.partition", 0)
  }

  // Called on executor side when writing rows
  def outputWriterForRow(row: Row): OutputWriter

  protected def initWriters(): Unit

  def commitTask(): Unit = {
    SparkHadoopMapRedUtil.commitTask(
      outputCommitter, taskAttemptContext, jobId.getId, taskId.getId, taskAttemptId.getId)
  }

  def abortTask(): Unit = {
    if (outputCommitter != null) {
      outputCommitter.abortTask(taskAttemptContext)
    }
    logError(s"Task attempt $taskAttemptId aborted.")
  }

  def commitJob(): Unit = {
    outputCommitter.commitJob(jobContext)
    logInfo(s"Job $jobId committed.")
  }

  def abortJob(): Unit = {
    if (outputCommitter != null) {
      outputCommitter.abortJob(jobContext, JobStatus.State.FAILED)
    }
    logError(s"Job $jobId aborted.")
  }
}

private[sql] class DefaultWriterContainer(
    @transient relation: HadoopFsRelation,
    @transient job: Job,
    isAppend: Boolean)
  extends BaseWriterContainer(relation, job, isAppend) {

  @transient private var writer: OutputWriter = _

  override protected def initWriters(): Unit = {
    taskAttemptContext.getConfiguration.set("spark.sql.sources.output.path", outputPath)
    writer = outputWriterFactory.newInstance(getWorkPath, dataSchema, taskAttemptContext)
  }

  override def outputWriterForRow(row: Row): OutputWriter = writer

  override def commitTask(): Unit = {
    try {
      assert(writer != null, "OutputWriter instance should have been initialized")
      writer.close()
      super.commitTask()
    } catch { case cause: Throwable =>
      // This exception will be handled in `InsertIntoHadoopFsRelation.insert$writeRows`, and will
      // cause `abortTask()` to be invoked.
      throw new RuntimeException("Failed to commit task", cause)
    }
  }

  override def abortTask(): Unit = {
    try {
      // It's possible that the task fails before `writer` gets initialized
      if (writer != null) {
        writer.close()
      }
    } finally {
      super.abortTask()
    }
  }
}

private[sql] class DynamicPartitionWriterContainer(
    @transient relation: HadoopFsRelation,
    @transient job: Job,
    partitionColumns: Array[String],
    defaultPartitionName: String,
    isAppend: Boolean)
  extends BaseWriterContainer(relation, job, isAppend) {

  // All output writers are created on executor side.
  @transient protected var outputWriters: mutable.Map[String, OutputWriter] = _

  override protected def initWriters(): Unit = {
    outputWriters = mutable.Map.empty[String, OutputWriter]
  }

  override def outputWriterForRow(row: Row): OutputWriter = {
    val partitionPath = partitionColumns.zip(row.toSeq).map { case (col, rawValue) =>
      val string = if (rawValue == null) null else String.valueOf(rawValue)
      val valueString = if (string == null || string.isEmpty) {
        defaultPartitionName
      } else {
        PartitioningUtils.escapePathName(string)
      }
      s"/$col=$valueString"
    }.mkString.stripPrefix(Path.SEPARATOR)

    outputWriters.getOrElseUpdate(partitionPath, {
      val path = new Path(getWorkPath, partitionPath)
      taskAttemptContext.getConfiguration.set(
        "spark.sql.sources.output.path",
        new Path(outputPath, partitionPath).toString)
      outputWriterFactory.newInstance(path.toString, dataSchema, taskAttemptContext)
    })
  }

  private def clearOutputWriters(): Unit = {
    if (outputWriters.nonEmpty) {
      outputWriters.values.foreach(_.close())
      outputWriters.clear()
    }
  }

  override def commitTask(): Unit = {
    try {
      clearOutputWriters()
      super.commitTask()
    } catch { case cause: Throwable =>
      throw new RuntimeException("Failed to commit task", cause)
    }
  }

  override def abortTask(): Unit = {
    try {
      clearOutputWriters()
    } finally {
      super.abortTask()
    }
  }
}
