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
package org.apache.spark.streaming.kinesis

import com.amazonaws.regions.RegionUtils
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.InitialPositionInStream

import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.api.java.{JavaReceiverInputDStream, JavaStreamingContext}
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.streaming.{Duration, StreamingContext}


object KinesisUtils {
  /**
   * Create an input stream that pulls messages from a Kinesis stream.
   * This uses the Kinesis Client Library (KCL) to pull messages from Kinesis.
   *
   * Note: The AWS credentials will be discovered using the DefaultAWSCredentialsProviderChain
   * on the workers. See AWS documentation to understand how DefaultAWSCredentialsProviderChain
   * gets the AWS credentials.
   *
   * @param ssc StreamingContext object
   * @param kinesisAppName  Kinesis application name used by the Kinesis Client Library
   *                        (KCL) to update DynamoDB
   * @param streamName   Kinesis stream name
   * @param endpointUrl  Url of Kinesis service (e.g., https://kinesis.us-east-1.amazonaws.com)
   * @param regionName   Name of region used by the Kinesis Client Library (KCL) to update
   *                     DynamoDB (lease coordination and checkpointing) and CloudWatch (metrics)
   * @param initialPositionInStream  In the absence of Kinesis checkpoint info, this is the
   *                                 worker's initial starting position in the stream.
   *                                 The values are either the beginning of the stream
   *                                 per Kinesis' limit of 24 hours
   *                                 (InitialPositionInStream.TRIM_HORIZON) or
   *                                 the tip of the stream (InitialPositionInStream.LATEST).
   * @param checkpointInterval  Checkpoint interval for Kinesis checkpointing.
   *                            See the Kinesis Spark Streaming documentation for more
   *                            details on the different types of checkpoints.
   * @param storageLevel Storage level to use for storing the received objects.
   *                     StorageLevel.MEMORY_AND_DISK_2 is recommended.
   */
  def createStream(
      ssc: StreamingContext,
      kinesisAppName: String,
      streamName: String,
      endpointUrl: String,
      regionName: String,
      initialPositionInStream: InitialPositionInStream,
      checkpointInterval: Duration,
      storageLevel: StorageLevel
    ): ReceiverInputDStream[Array[Byte]] = {
    // Setting scope to override receiver stream's scope of "receiver stream"
    ssc.withNamedScope("kinesis stream") {
      ssc.receiverStream(
        new KinesisReceiver(kinesisAppName, streamName, endpointUrl, validateRegion(regionName),
          initialPositionInStream, checkpointInterval, storageLevel, None))
    }
  }

  /**
   * Create an input stream that pulls messages from a Kinesis stream.
   * This uses the Kinesis Client Library (KCL) to pull messages from Kinesis.
   *
   * Note:
   *  The given AWS credentials will get saved in DStream checkpoints if checkpointing
   *  is enabled. Make sure that your checkpoint directory is secure.
   *
   * @param ssc StreamingContext object
   * @param kinesisAppName  Kinesis application name used by the Kinesis Client Library
   *                        (KCL) to update DynamoDB
   * @param streamName   Kinesis stream name
   * @param endpointUrl  Url of Kinesis service (e.g., https://kinesis.us-east-1.amazonaws.com)
   * @param regionName   Name of region used by the Kinesis Client Library (KCL) to update
   *                     DynamoDB (lease coordination and checkpointing) and CloudWatch (metrics)
   * @param awsAccessKeyId  AWS AccessKeyId (if null, will use DefaultAWSCredentialsProviderChain)
   * @param awsSecretKey  AWS SecretKey (if null, will use DefaultAWSCredentialsProviderChain)
   * @param checkpointInterval  Checkpoint interval for Kinesis checkpointing.
   *                            See the Kinesis Spark Streaming documentation for more
   *                            details on the different types of checkpoints.
   * @param initialPositionInStream  In the absence of Kinesis checkpoint info, this is the
   *                                 worker's initial starting position in the stream.
   *                                 The values are either the beginning of the stream
   *                                 per Kinesis' limit of 24 hours
   *                                 (InitialPositionInStream.TRIM_HORIZON) or
   *                                 the tip of the stream (InitialPositionInStream.LATEST).
   * @param storageLevel Storage level to use for storing the received objects.
   *                     StorageLevel.MEMORY_AND_DISK_2 is recommended.
   */
  def createStream(
      ssc: StreamingContext,
      kinesisAppName: String,
      streamName: String,
      endpointUrl: String,
      regionName: String,
      initialPositionInStream: InitialPositionInStream,
      checkpointInterval: Duration,
      storageLevel: StorageLevel,
      awsAccessKeyId: String,
      awsSecretKey: String
    ): ReceiverInputDStream[Array[Byte]] = {
    ssc.receiverStream(
      new KinesisReceiver(kinesisAppName, streamName, endpointUrl, validateRegion(regionName),
        initialPositionInStream, checkpointInterval, storageLevel,
        Some(SerializableAWSCredentials(awsAccessKeyId, awsSecretKey))))
  }

  /**
   * Create an input stream that pulls messages from a Kinesis stream.
   * This uses the Kinesis Client Library (KCL) to pull messages from Kinesis.
   *
   * Note:
   * - The AWS credentials will be discovered using the DefaultAWSCredentialsProviderChain
   *   on the workers. See AWS documentation to understand how DefaultAWSCredentialsProviderChain
   *   gets AWS credentials.
   * - The region of the `endpointUrl` will be used for DynamoDB and CloudWatch.
   * - The Kinesis application name used by the Kinesis Client Library (KCL) will be the app name in
   *   [[org.apache.spark.SparkConf]].
   *
   * @param ssc Java StreamingContext object
   * @param streamName   Kinesis stream name
   * @param endpointUrl  Endpoint url of Kinesis service
   *                     (e.g., https://kinesis.us-east-1.amazonaws.com)
   * @param checkpointInterval  Checkpoint interval for Kinesis checkpointing.
   *                            See the Kinesis Spark Streaming documentation for more
   *                            details on the different types of checkpoints.
   * @param initialPositionInStream  In the absence of Kinesis checkpoint info, this is the
   *                                 worker's initial starting position in the stream.
   *                                 The values are either the beginning of the stream
   *                                 per Kinesis' limit of 24 hours
   *                                 (InitialPositionInStream.TRIM_HORIZON) or
   *                                 the tip of the stream (InitialPositionInStream.LATEST).
   * @param storageLevel Storage level to use for storing the received objects
   *                     StorageLevel.MEMORY_AND_DISK_2 is recommended.
   */
  @deprecated("use other forms of createStream", "1.4.0")
  def createStream(
      ssc: StreamingContext,
      streamName: String,
      endpointUrl: String,
      checkpointInterval: Duration,
      initialPositionInStream: InitialPositionInStream,
      storageLevel: StorageLevel
    ): ReceiverInputDStream[Array[Byte]] = {
    ssc.receiverStream(
      new KinesisReceiver(ssc.sc.appName, streamName, endpointUrl, getRegionByEndpoint(endpointUrl),
        initialPositionInStream, checkpointInterval, storageLevel, None))
  }

  /**
   * Create an input stream that pulls messages from a Kinesis stream.
   * This uses the Kinesis Client Library (KCL) to pull messages from Kinesis.
   *
   * Note: The AWS credentials will be discovered using the DefaultAWSCredentialsProviderChain
   * on the workers. See AWS documentation to understand how DefaultAWSCredentialsProviderChain
   * gets the AWS credentials.
   *
   * @param jssc Java StreamingContext object
   * @param kinesisAppName  Kinesis application name used by the Kinesis Client Library
   *                        (KCL) to update DynamoDB
   * @param streamName   Kinesis stream name
   * @param endpointUrl  Url of Kinesis service (e.g., https://kinesis.us-east-1.amazonaws.com)
   * @param regionName   Name of region used by the Kinesis Client Library (KCL) to update
   *                     DynamoDB (lease coordination and checkpointing) and CloudWatch (metrics)
   * @param checkpointInterval  Checkpoint interval for Kinesis checkpointing.
   *                            See the Kinesis Spark Streaming documentation for more
   *                            details on the different types of checkpoints.
   * @param initialPositionInStream  In the absence of Kinesis checkpoint info, this is the
   *                                 worker's initial starting position in the stream.
   *                                 The values are either the beginning of the stream
   *                                 per Kinesis' limit of 24 hours
   *                                 (InitialPositionInStream.TRIM_HORIZON) or
   *                                 the tip of the stream (InitialPositionInStream.LATEST).
   * @param storageLevel Storage level to use for storing the received objects.
   *                     StorageLevel.MEMORY_AND_DISK_2 is recommended.
   */
  def createStream(
      jssc: JavaStreamingContext,
      kinesisAppName: String,
      streamName: String,
      endpointUrl: String,
      regionName: String,
      initialPositionInStream: InitialPositionInStream,
      checkpointInterval: Duration,
      storageLevel: StorageLevel
    ): JavaReceiverInputDStream[Array[Byte]] = {
    createStream(jssc.ssc, kinesisAppName, streamName, endpointUrl, regionName,
      initialPositionInStream, checkpointInterval, storageLevel)
  }

  /**
   * Create an input stream that pulls messages from a Kinesis stream.
   * This uses the Kinesis Client Library (KCL) to pull messages from Kinesis.
   *
   * Note:
   *  The given AWS credentials will get saved in DStream checkpoints if checkpointing
   *  is enabled. Make sure that your checkpoint directory is secure.
   *
   * @param jssc Java StreamingContext object
   * @param kinesisAppName  Kinesis application name used by the Kinesis Client Library
   *                        (KCL) to update DynamoDB
   * @param streamName   Kinesis stream name
   * @param endpointUrl  Url of Kinesis service (e.g., https://kinesis.us-east-1.amazonaws.com)
   * @param regionName   Name of region used by the Kinesis Client Library (KCL) to update
   *                     DynamoDB (lease coordination and checkpointing) and CloudWatch (metrics)
   * @param awsAccessKeyId  AWS AccessKeyId (if null, will use DefaultAWSCredentialsProviderChain)
   * @param awsSecretKey  AWS SecretKey (if null, will use DefaultAWSCredentialsProviderChain)
   * @param checkpointInterval  Checkpoint interval for Kinesis checkpointing.
   *                            See the Kinesis Spark Streaming documentation for more
   *                            details on the different types of checkpoints.
   * @param initialPositionInStream  In the absence of Kinesis checkpoint info, this is the
   *                                 worker's initial starting position in the stream.
   *                                 The values are either the beginning of the stream
   *                                 per Kinesis' limit of 24 hours
   *                                 (InitialPositionInStream.TRIM_HORIZON) or
   *                                 the tip of the stream (InitialPositionInStream.LATEST).
   * @param storageLevel Storage level to use for storing the received objects.
   *                     StorageLevel.MEMORY_AND_DISK_2 is recommended.
   */
  def createStream(
      jssc: JavaStreamingContext,
      kinesisAppName: String,
      streamName: String,
      endpointUrl: String,
      regionName: String,
      initialPositionInStream: InitialPositionInStream,
      checkpointInterval: Duration,
      storageLevel: StorageLevel,
      awsAccessKeyId: String,
      awsSecretKey: String
    ): JavaReceiverInputDStream[Array[Byte]] = {
    createStream(jssc.ssc, kinesisAppName, streamName, endpointUrl, regionName,
        initialPositionInStream, checkpointInterval, storageLevel, awsAccessKeyId, awsSecretKey)
  }

  /**
   * Create an input stream that pulls messages from a Kinesis stream.
   * This uses the Kinesis Client Library (KCL) to pull messages from Kinesis.
   *
   * Note:
   * - The AWS credentials will be discovered using the DefaultAWSCredentialsProviderChain
   *   on the workers. See AWS documentation to understand how DefaultAWSCredentialsProviderChain
   *   gets AWS credentials.
   * - The region of the `endpointUrl` will be used for DynamoDB and CloudWatch.
   * - The Kinesis application name used by the Kinesis Client Library (KCL) will be the app name in
   *   [[org.apache.spark.SparkConf]].
   *
   * @param jssc Java StreamingContext object
   * @param streamName   Kinesis stream name
   * @param endpointUrl  Endpoint url of Kinesis service
   *                     (e.g., https://kinesis.us-east-1.amazonaws.com)
   * @param checkpointInterval  Checkpoint interval for Kinesis checkpointing.
   *                            See the Kinesis Spark Streaming documentation for more
   *                            details on the different types of checkpoints.
   * @param initialPositionInStream  In the absence of Kinesis checkpoint info, this is the
   *                                 worker's initial starting position in the stream.
   *                                 The values are either the beginning of the stream
   *                                 per Kinesis' limit of 24 hours
   *                                 (InitialPositionInStream.TRIM_HORIZON) or
   *                                 the tip of the stream (InitialPositionInStream.LATEST).
   * @param storageLevel Storage level to use for storing the received objects
   *                     StorageLevel.MEMORY_AND_DISK_2 is recommended.
   */
  @deprecated("use other forms of createStream", "1.4.0")
  def createStream(
      jssc: JavaStreamingContext,
      streamName: String,
      endpointUrl: String,
      checkpointInterval: Duration,
      initialPositionInStream: InitialPositionInStream,
      storageLevel: StorageLevel
    ): JavaReceiverInputDStream[Array[Byte]] = {
    createStream(
      jssc.ssc, streamName, endpointUrl, checkpointInterval, initialPositionInStream, storageLevel)
  }

  private def getRegionByEndpoint(endpointUrl: String): String = {
    RegionUtils.getRegionByEndpoint(endpointUrl).getName()
  }

  private def validateRegion(regionName: String): String = {
    Option(RegionUtils.getRegion(regionName)).map { _.getName }.getOrElse {
      throw new IllegalArgumentException(s"Region name '$regionName' is not valid")
    }
  }
}
