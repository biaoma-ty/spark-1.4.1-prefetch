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

package org.apache.spark.network.netty

import java.nio.ByteBuffer
import java.util.concurrent.LinkedBlockingQueue
import scala.collection.JavaConversions._
import org.apache.spark.{SparkEnv, Logging}
import org.apache.spark.network.BlockDataManager
import org.apache.spark.network.buffer.{ManagedBuffer, NioManagedBuffer}
import org.apache.spark.network.client.{RpcResponseCallback, TransportClient}
import org.apache.spark.network.server.{OneForOneStreamManager, RpcHandler, StreamManager}
import org.apache.spark.network.shuffle.protocol._
import org.apache.spark.serializer.Serializer
import org.apache.spark.storage.{BlockId, StorageLevel}
import scala.collection.mutable.{ArrayBuffer, HashMap}

/**
 * Serves requests to open blocks by simply registering one chunk per block requested.
 * Handles opening and uploading arbitrary BlockManager blocks.
 *
 * Opened blocks are registered with the "one-for-one" strategy, meaning each Transport-layer Chunk
 * is equivalent to one Spark-level shuffle block.
 */
class NettyBlockRpcServer(
    serializer: Serializer,
    blockManager: BlockDataManager)
  extends RpcHandler with Logging {

  private val streamManager = new OneForOneStreamManager()

  private val maxBytesInBuffer = SparkEnv.get.conf.getSizeAsMb("spark.reducer.maxSizeInFlight", "48m")*1024*1024

  def getReduceId(blockId: BlockId): Int = {
    val ids = blockId.name.split("_|\\.")
    val reduceId = Integer.valueOf(ids(3))
    reduceId
  }

  override def receive(
      client: TransportClient,
      messageBytes: Array[Byte],
      responseContext: RpcResponseCallback): Unit = {
    val message = BlockTransferMessage.Decoder.fromByteArray(messageBytes)
    logTrace(s"Received request: $message")

    val openedBlocks = new HashMap[ClientInfo, BlocksInfo]

    val requestQueue = new LinkedBlockingQueue[BlocksInfo]

    message match {
      case openBlocks: OpenBlocks =>
//        val blocks: Seq[ManagedBuffer] =
//          openBlocks.blockIds.map(BlockId.apply).map(blockManager.getBlockData)
//        val streamId = streamManager.registerStream(blocks.iterator)
//        logTrace(s"Registered streamId $streamId with ${blocks.size} buffers")
//        responseContext.onSuccess(new StreamHandle(streamId, blocks.size).toByteArray)

        val blockIds = openBlocks.blockIds.map(BlockId.apply)

        var streamId = 0L
        if (blockIds(0).isShuffle) {
          logDebug(s"get the prepared block " + blockIds(0))
          val queue = new LinkedBlockingQueue[ManagedBuffer]()
          blockIds.foreach(blockId =>{
            logDebug(s"BM@Server getting block " + blockId.name)
            queue.add(BlockCache.get(blockId))
          })
//          val queue = BlockCache.getAll(blockIds)
          streamId = streamManager.registerStream(queue.iterator())
        } else {
          val blocks: Seq[ManagedBuffer] =
            openBlocks.blockIds.map(BlockId.apply).map(blockManager.getBlockData)

          streamId = streamManager.registerStream(blocks.iterator)
        }
        responseContext.onSuccess(new StreamHandle(streamId, blockIds.size).toByteArray)

      case uploadBlock: UploadBlock =>
        // StorageLevel is serialized as bytes using our JavaSerializer.
        val level: StorageLevel =
          serializer.newInstance().deserialize(ByteBuffer.wrap(uploadBlock.metadata))
        val data = new NioManagedBuffer(ByteBuffer.wrap(uploadBlock.blockData))
        blockManager.putBlockData(BlockId(uploadBlock.blockId), data, level)
        responseContext.onSuccess(new Array[Byte](0))

      case prepareBlocks: PrepareBlocks =>

        if (prepareBlocks.blockIdsToRelease.size > 0){
          val blocksToRelease: Seq[BlockId] =
            prepareBlocks.blockIdsToRelease.map(BlockId.apply)
          BlockCache.release(blocksToRelease.toArray)
        }

        val blockIds: Seq[BlockId] =
          prepareBlocks.blockIds.map(BlockId.apply)
        BlockCache.putAll(blockIds.toArray)

        val reduceId:Int = Integer.valueOf(blockIds(0).name.split("_|\\.")(3))
        val shuffleId:Int = Integer.valueOf(blockIds(0).name.split("_|\\.")(1))
        //initialize clientInfo
        val clientInfo:ClientInfo = new ClientInfo(shuffleId, reduceId)

        //intialize blocksInfo
        val blocksInfo = new BlocksInfo
        val blocksSize = blockIds.foldLeft(0L)((size, i) => size + blockManager.getBlockData(i).size())
        logDebug(s"get prepare message for blocks $blockIds size $blocksSize" )
        logInfo(s"BM@Server get release message for blocks " + prepareBlocks.blockIdsToRelease.map(BlockId.apply))
        val blockIdsBuf = ArrayBuffer(blockIds: _*)
//        blocksInfo.append(blockIdsBuf,blocksSize)


        //policy one ReleaseByCount
//        if (openedBlocks.contains(clientInfo)){
//          //Do not need to out another same client added by mabiaocas@gmail.com
//          openedBlocks.get(clientInfo).get.append(blockIdsBuf, blocksSize)
//        } else {
//          openedBlocks.put(clientInfo, blocksInfo)
//        }

        // add to cache and request queue(for release later)
        requestQueue.put(blocksInfo)

        //check whether satisfy the release condition
//        if (openedBlocks.get(clientInfo).get.size >= maxBytesInBuffer){
//          val requestIter = requestQueue.take().getBlocksIterator()
//          logDebug("BM@Server buffer collect")
//          while (requestIter.hasNext){
//            BlockCache.release(requestIter.next().toArray)
//          }
//        }

        logDebug("async add future buffer finished added buffer's size : " + blocksSize)
        responseContext.onSuccess(new Array[Byte](0))
    }
  }
  override def getStreamManager(): StreamManager = streamManager
}