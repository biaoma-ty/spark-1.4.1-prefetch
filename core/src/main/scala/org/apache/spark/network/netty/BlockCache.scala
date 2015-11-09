package org.apache.spark.network.netty

import java.util.concurrent._
import org.apache.spark.storage.BlockId
import org.apache.spark.{SparkEnv, Logging}
import org.apache.spark.network.buffer.{NioManagedBuffer, NettyManagedBuffer, ManagedBuffer}

/**
 * Created by INFI on 2015/9/18.
 */
object BlockCache extends  Logging{

  val reqBuffer = new ConcurrentHashMap[Seq[BlockId], FutureCacheForBLocks]()

  def releaseAll(blockIds: Array[BlockId]): Unit ={

    //    val dataCollections = getAll(blockIds)
    //    while ((dataCollections != null) && !dataCollections.isEmpty) {
    //      val data = dataCollections.peek()
    //      data.release()
    //    }
    reqBuffer.get(blockIds).release()
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

  def containsAll(blockIds: Seq[BlockId]): Boolean ={
    if (reqBuffer.containsKey(blockIds))
      return true
    return  false
  }
}

class FutureCacheForBLocks {
  var blockIds: Seq[BlockId] = _
  var future:FutureTask[LinkedBlockingQueue[ManagedBuffer]] = _
  var real:RealCacheForBlocks = _

  def this (blockIds: Seq[BlockId]) {
    this()
    this.blockIds = blockIds
    real = new RealCacheForBlocks(blockIds)
    future = new FutureTask[LinkedBlockingQueue[ManagedBuffer]](real)

    val executor = Executors.newCachedThreadPool()

    executor.submit(future)
  }

  def get():LinkedBlockingQueue[ManagedBuffer] = {
    future.get()
  }

  def release(): Unit ={
    real.release()
  }
}

class RealCacheForBlocks extends  Callable[LinkedBlockingQueue[ManagedBuffer]] {
  val blockManager = SparkEnv.get.blockManager
  var blockIds: Seq[BlockId] = _
  val resQueue = new LinkedBlockingQueue[ManagedBuffer]()

  def this(blockIds: Seq[BlockId]) {
    this()
    this.blockIds = blockIds
  }

  def release(): Unit ={
    resQueue.removeAll(resQueue)
  }

  override def call(): LinkedBlockingQueue[ManagedBuffer] = {
    val iterator = blockIds.iterator
    while (iterator.hasNext){
      val blockId = iterator.next()
      if (blockId != null) {
        val data = blockManager.getBlockData(blockId)
        resQueue.add(new NioManagedBuffer(data.nioByteBuffer()))
      }
    }
    resQueue
  }
}