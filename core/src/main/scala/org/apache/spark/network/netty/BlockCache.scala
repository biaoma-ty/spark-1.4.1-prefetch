package org.apache.spark.network.netty

import java.util.concurrent._
import org.apache.spark.storage.BlockId
import org.apache.spark.{SparkEnv, Logging}
import org.apache.spark.network.buffer.ManagedBuffer

/**
 * Created by INFI on 2015/9/18.
 */
object BlockCache extends  Logging{

  val reqBuffer = new ConcurrentHashMap[Seq[BlockId], FutureCacheForBLocks]()

  def releaseAll(blockIds: Array[BlockId]): Unit ={
    reqBuffer.remove(blockIds)
  }

  def addAll(blockIds: Seq[BlockId]): Unit = {
    val data = new FutureCacheForBLocks(blockIds)
    reqBuffer.put(blockIds, data)
  }

  def getAll(blockIds: Seq[BlockId]): LinkedBlockingQueue[ManagedBuffer] = {
    val buffers = reqBuffer.get(blockIds)
    buffers.get()
  }
}

class FutureCacheForBLocks {
  var blockIds: Seq[BlockId] = _
  var future:FutureTask[LinkedBlockingQueue[ManagedBuffer]] = _

  def this (blockIds: Seq[BlockId]) {
    this()
    this.blockIds = blockIds
    future = new FutureTask[LinkedBlockingQueue[ManagedBuffer]](new RealCacheForBlocks(blockIds))

    val executor = Executors.newFixedThreadPool(1)

    executor.submit(future)
  }

  def get():LinkedBlockingQueue[ManagedBuffer] = {
    future.get()
  }
}

class RealCacheForBlocks extends  Callable[LinkedBlockingQueue[ManagedBuffer]] {
  val blockManager = SparkEnv.get.blockManager
  var blockIds: Seq[BlockId] = _

  def this(blockIds: Seq[BlockId]) {
    this()
    this.blockIds = blockIds
  }

  override def call(): LinkedBlockingQueue[ManagedBuffer] = {
    val resQueue = new LinkedBlockingQueue[ManagedBuffer]()
    val iterator = blockIds.iterator
    while (iterator.hasNext){
      val blockId = iterator.next()
      if (blockId != null) {
        val data = blockManager.getBlockData(blockId)
        resQueue.add(data)
      }
    }
    resQueue
  }
}