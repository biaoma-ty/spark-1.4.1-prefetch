package org.apache.spark.network.netty

import java.util

import org.apache.spark.Logging
import org.apache.spark.storage.BlockId

import scala.collection.mutable.ArrayBuffer

/**
 * Created by INFI on 2015/9/24.
 */
class ClientInfo extends Logging{

  var shuffleId: Int = _
  var reduceId: Int = _

  def this(shuffleId: Int, reduceId: Int){
    this()
    this.shuffleId = shuffleId
    this.reduceId = reduceId
  }

  override def equals(obj: scala.Any): Boolean = {
    if (!obj.isInstanceOf[ClientInfo])
      return false
    val c = obj.asInstanceOf[ClientInfo]
      return c.shuffleId == this.shuffleId && c.reduceId == this.reduceId
  }

  override def hashCode(): Int = {
    return reduceId >> shuffleId
  }
}

class BlocksInfo extends  Logging{
  var blocks: ArrayBuffer[ArrayBuffer[BlockId]] = new ArrayBuffer[ArrayBuffer[BlockId]]()
  var size: Long = _

  def append(blockIds: ArrayBuffer[BlockId],size: Long): Unit ={
    logDebug(s"BM@BlocksInfo blockIds: $blockIds size: $size")
    this.size += size
    this.blocks += blockIds
  }

  def getBlocksIterator()= {
    blocks.iterator
  }
}

class BlockBufferMap extends util.HashMap with Logging{
  override def get(key: scala.Any): Nothing = {
    super.get(key)
  }

  override def put(key: Nothing, value: Nothing): Nothing = {
    super.put(key, value)
  }
}
