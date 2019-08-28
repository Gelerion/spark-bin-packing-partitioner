package com.gelerion.spark.bin.packing.partitioner.spark.partitioner

import com.gelerion.spark.bin.packing.partitioner.domain.{BookshelfUrl, EBooksUrls}
import org.apache.spark.{HashPartitioner, Partitioner}
import org.apache.spark.sql.Dataset
import org.apache.spark.util.Utils

import scala.collection.immutable.ListMap
import scala.collection.mutable


class BinPackingPartitioner(partitions: Int) extends Partitioner {
  require(partitions >= 0, s"Number of partitions ($partitions) cannot be negative.")

  def numPartitions: Int = partitions

  def getPartition(key: Any): Int = key match {
    case null => 0
    case _ => //implement
      1
  }

  override def equals(other: Any): Boolean = other match {
    case h: HashPartitioner =>
      h.numPartitions == numPartitions
    case _ =>
      false
  }

  override def hashCode: Int = numPartitions
}

object PartMain {
  def main(args: Array[String]): Unit = {
    //EBooksUrls()
  }
}