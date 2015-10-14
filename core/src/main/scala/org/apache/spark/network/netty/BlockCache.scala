package org.apache.spark.network.netty

import java.util.concurrent._
import org.apache.spark.storage.BlockId
import org.apache.spark.{SparkEnv, Logging}
import org.apache.spark.network.buffer.ManagedBuffer
import scala.collection.mutable

/**
 * Created by INFI on 2015/9/18.
 */
object BlockCache extends  Logging{

  val blocksCache = new mutable.HashMap[BlockId,FutureCache]()

  def put(blockId: BlockId): Unit = {
    val data = new FutureCache(blockId)
    blocksCache.put(blockId, data)
  }

  def putAll(blockIds: Array[BlockId]): Unit ={
    blockIds.foreach(blockId => put(blockId))
  }

  def get(blockId: BlockId): ManagedBuffer ={
    val data = blocksCache.get(blockId).get
    data.get()
  }

  def release(blockIds: Array[BlockId]): Unit ={
    blockIds.foreach(blockId =>{
      val cache = blocksCache.get(blockId).get
      blocksCache.remove(blockId)
      cache.get().release()
    })
  }

  def getAll(blockIds: Array[BlockId]): LinkedBlockingQueue[ManagedBuffer] ={
    val cacheQueue = new LinkedBlockingQueue[ManagedBuffer]()
    blockIds.foreach(blockId => {
      cacheQueue.add(get(blockId))
    })
    cacheQueue
  }
}

class FutureCache{
  var blockId: BlockId = _
  var future: FutureTask[ManagedBuffer] = _
  def this(blockId: BlockId) {
    this()
    this.blockId = blockId
    future = new FutureTask[ManagedBuffer](new RealCache(blockId))

    val executor = Executors.newFixedThreadPool(1)

    executor.submit(future)
  }

  def get(): ManagedBuffer = {
    future.get()
  }
}

class RealCache extends  Callable[ManagedBuffer] {
  val blockManager = SparkEnv.get.blockManager
  var blockId:BlockId = _
  def this(blockId: BlockId) {
    this()
    this.blockId = blockId
  }



  override def call(): ManagedBuffer = {
    blockManager.getBlockData(blockId)
  }
}
