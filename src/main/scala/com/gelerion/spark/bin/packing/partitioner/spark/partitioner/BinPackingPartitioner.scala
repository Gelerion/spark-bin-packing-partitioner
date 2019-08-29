package com.gelerion.spark.bin.packing.partitioner.spark.partitioner

import com.gelerion.spark.bin.packing.partitioner.domain.model.BookshelfUrl
import com.gelerion.spark.bin.packing.partitioner.service.BinsContainer
import org.apache.spark.Partitioner


class BinPackingPartitioner(packedUrls: BinsContainer[BookshelfUrl]) extends Partitioner {

  def numPartitions: Int = packedUrls.nbins

  def getPartition(key: Any): Int = key match {
    case null => 0
    case _ => packedUrls.lookupItemIdx(key.asInstanceOf[BookshelfUrl]).get
  }

  override def equals(other: Any): Boolean = other match {
    case p: BinPackingPartitioner => p.numPartitions == numPartitions
    case _ => false
  }

  override def hashCode: Int = numPartitions
}

object PartMain {
  def main(args: Array[String]): Unit = {
    //EBooksUrls()
  }
}