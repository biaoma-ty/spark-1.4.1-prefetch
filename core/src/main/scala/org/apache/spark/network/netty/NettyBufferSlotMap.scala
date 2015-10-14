package org.apache.spark.network.netty

import java.util.concurrent.{LinkedBlockingQueue, LinkedBlockingDeque}

import org.apache.spark.Logging
import org.apache.spark.network.buffer.ManagedBuffer
import org.apache.spark.storage.BlockId

import scala.collection.mutable

/**
 * Created by INFI on 2015/9/17.
 */
object NettyBufferSlotMap extends  mutable.HashMap{

  def parseBlocks(blockId: BlockId): (Int, Int) = {
    val blockName = blockId.name
    val ids = blockName.split("_|\\.")
    val shuffleId = Integer.valueOf(ids(1))
    val reduceId = Integer.valueOf(ids(3))

    (shuffleId, reduceId)
  }

  def put(blockId: BlockId): Unit ={
    val shuffleId = parseBlocks(blockId)._1
    val reduceId = parseBlocks(blockId)._2

    super.put((shuffleId, reduceId), blockId)
  }
}
